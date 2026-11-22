#!/usr/bin/env bash
# Chaos-проверка: SIGKILL consumer'а посреди обработки платежа.
#
# Сценарий (воспроизводит то, что README называет «жёсткое падение»):
#   1. consumer перезапускается с медленным шлюзом (20 с), чтобы гарантированно
#      попасть в окно обработки;
#   2. создаётся платёж, через 3 с consumer убивается `docker kill --signal=KILL`
#      (клейм в БД остаётся, release_claim не выполняется);
#   3. consumer поднимается заново; сообщение возвращается в очередь;
#   4. дубликат ждёт протухания клейма (PAYMENT_CLAIM_TTL_SECONDS≈30),
#      перехватывает его и доводит платёж до финального статуса.
#
# Ожидаемый результат: платёж succeeded/failed (не pending), retry-шума и
# записей в payments.dlq нет. Код возврата 0 — PASS.
#
# Требования: поднятый `docker compose up -d`, curl, docker.
set -euo pipefail

cd "$(dirname "$0")/.."

COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-payment-service}"
export COMPOSE_PROJECT_NAME
compose() {
  docker compose --project-name "$COMPOSE_PROJECT_NAME" "$@"
}

# Контейнер пересоздаётся ниже, а compose подставляет ${PAYMENT_WEBHOOK_ALLOW_PRIVATE_HOSTS}
# заново из окружения вызывающего. Унаследованный флаг сохраняем, иначе consumer
# вернётся с запретом приватных адресов и последующие webhook-сценарии получат
# «private address … for host 'host.docker.internal'» вместо HTTP-запроса.
ALLOW_PRIVATE="${PAYMENT_WEBHOOK_ALLOW_PRIVATE_HOSTS:-}"
if [ -z "$ALLOW_PRIVATE" ]; then
  ALLOW_PRIVATE="$(compose exec -T consumer printenv PAYMENT_WEBHOOK_ALLOW_PRIVATE_HOSTS 2>/dev/null || echo false)"
fi
export PAYMENT_WEBHOOK_ALLOW_PRIVATE_HOSTS="${ALLOW_PRIVATE:-false}"

API_URL="${PAYMENT_API_URL:-http://localhost:8000}"
API_KEY="${PAYMENT_API_KEY:-local-dev-key}"
GATEWAY_SECONDS="${CHAOS_GATEWAY_SECONDS:-20}"
TIMEOUT_SECONDS="${CHAOS_TIMEOUT_SECONDS:-120}"
WEBHOOK_PORT="${CHAOS_WEBHOOK_PORT:-9001}"
DLQ_QUEUE="${PAYMENT_RABBITMQ_DLQ:-payments.dlq}"

TMP_ROOT="${TMPDIR:-/tmp}"
OVERRIDE="$(mktemp "$TMP_ROOT/chaos-kill9-override.XXXXXX")"
SINK_LOG="$(mktemp "$TMP_ROOT/chaos-kill9-webhook.XXXXXX")"
HEALTH_TOKEN="$(python3 -c 'import secrets; print(secrets.token_hex(16))')"
SINK_PID=""
CONSUMER=""
POSTGRES=""
RABBITMQ=""

cat >"$OVERRIDE" <<YAML
services:
  consumer:
    environment:
      PAYMENT_PROCESS_MIN_SECONDS: "${GATEWAY_SECONDS}"
      PAYMENT_PROCESS_MAX_SECONDS: "${GATEWAY_SECONDS}"
YAML

restore_consumer() {
  [ -n "$CONSUMER" ] || return 0
  echo "==> восстанавливаю consumer в штатной конфигурации"
  compose -f docker-compose.yml -f "$OVERRIDE" down consumer >/dev/null 2>&1 || true
  compose up -d --no-deps consumer >/dev/null 2>&1 || true
}

stop_webhook_sink() {
  if [ -n "$SINK_PID" ]; then
    kill "$SINK_PID" >/dev/null 2>&1 || true
    wait "$SINK_PID" >/dev/null 2>&1 || true
  fi
  rm -f "$OVERRIDE" "$SINK_LOG"
}

cleanup() {
  restore_consumer
  stop_webhook_sink
}
trap cleanup EXIT

fail() {
  echo "FAIL: $*" >&2
  compose logs --tail 40 consumer >&2 || true
  exit 1
}

psql_value() {
  # $1 — SQL-выражение, возвращающее скаляр
  docker exec "$POSTGRES" psql -U payment -d payments -t -A \
    -c "SELECT $1 FROM payments WHERE id = '$PAYMENT_ID';"
}

dlq_count() {
  local output rc line name count found
  output="$(docker exec "$RABBITMQ" rabbitmqctl list_queues name messages 2>/dev/null)" || {
    rc=$?
    echo "FAIL: не удалось прочитать DLQ '$DLQ_QUEUE' (rabbitmqctl exit $rc)" >&2
    return 1
  }
  found=""
  while read -r name count _; do
    # Разрешаем ровно имя очереди и неотрицательное целое число.
    case "$count" in
      ''|*[!0-9]*) continue ;;
    esac
    if [ "$name" = "$DLQ_QUEUE" ]; then
      found="$count"
      break
    fi
  done <<EOF
$output
EOF
  if [ -z "$found" ]; then
    echo "FAIL: очередь DLQ '$DLQ_QUEUE' отсутствует или её счётчик некорректен" >&2
    return 1
  fi
  printf '%s\n' "$found"
}

start_webhook_sink() {
  python3 -u -c '
from http.server import BaseHTTPRequestHandler, HTTPServer
import sys

port, health_token = int(sys.argv[1]), sys.argv[2]
class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path != "/health/" + health_token:
            self.send_error(404)
            return
        self.send_response(200)
        self.end_headers()

    def do_POST(self):
        self.rfile.read(int(self.headers.get("Content-Length", "0")))
        self.send_response(200)
        self.end_headers()

    def log_message(self, fmt, *args):
        pass

HTTPServer(("0.0.0.0", port), Handler).serve_forever()
' "$WEBHOOK_PORT" "$HEALTH_TOKEN" >"$SINK_LOG" 2>&1 &
  SINK_PID=$!
  for _ in $(seq 1 50); do
    if ! kill -0 "$SINK_PID" 2>/dev/null; then
      cat "$SINK_LOG" >&2
      fail "webhook sink не запустился на порту $WEBHOOK_PORT"
    fi
    if curl -fsS "http://127.0.0.1:$WEBHOOK_PORT/health/$HEALTH_TOKEN" >/dev/null 2>&1; then
      echo "==> webhook sink отвечает на порту $WEBHOOK_PORT"
      return
    fi
    sleep 0.2
  done
  fail "webhook sink не стал готов за 10 секунд"
}

wait_for_webhook_delivery() {
  local deadline status
  deadline=$(( $(date +%s) + TIMEOUT_SECONDS ))
  status=""
  while [ "$(date +%s)" -lt "$deadline" ]; do
    status="$(docker exec "$POSTGRES" psql -U payment -d payments -t -A \
      -c "SELECT status FROM webhook_deliveries WHERE payment_id = '$PAYMENT_ID' ORDER BY created_at DESC LIMIT 1;")"
    [ "$status" = "delivered" ] && return 0
    [ "$status" = "failed" ] && fail "webhook-доставка платежа завершилась failed"
    sleep 1
  done
  fail "webhook-доставка платежа не завершилась за ${TIMEOUT_SECONDS}s (status=${status:-missing})"
}

start_webhook_sink

CONSUMER="$(compose ps -q consumer)"
POSTGRES="$(compose ps -q postgres)"
RABBITMQ="$(compose ps -q rabbitmq)"
[ -n "$CONSUMER" ] || fail "Compose consumer container не найден для проекта $COMPOSE_PROJECT_NAME"
[ -n "$POSTGRES" ] || fail "Compose postgres container не найден для проекта $COMPOSE_PROJECT_NAME"
[ -n "$RABBITMQ" ] || fail "Compose rabbitmq container не найден для проекта $COMPOSE_PROJECT_NAME"

DLQ_BEFORE="$(dlq_count)" || fail "не удалось проверить DLQ до сценария"
echo "==> $DLQ_QUEUE до сценария: $DLQ_BEFORE"

echo "==> consumer с медленным шлюзом (${GATEWAY_SECONDS}s)"
compose -f docker-compose.yml -f "$OVERRIDE" up -d --no-deps consumer >/dev/null
CONSUMER="$(compose ps -q consumer)"
sleep 4

echo "==> создаю платёж"
RESPONSE="$(curl -sf -X POST "$API_URL/api/v1/payments" \
  -H "X-API-Key: $API_KEY" \
  -H "Idempotency-Key: chaos-kill9-$(python3 -c 'import uuid; print(uuid.uuid4())')" \
  -H "Content-Type: application/json" \
  -d "{\"amount\":\"13.37\",\"currency\":\"RUB\",\"description\":\"chaos kill -9\",\
       \"metadata\":{\"check\":\"chaos\"},\"webhook_url\":\"http://host.docker.internal:$WEBHOOK_PORT/webhook\"}")" \
  || fail "не удалось создать платёж (стек поднят?)"
PAYMENT_ID="$(printf '%s' "$RESPONSE" | sed -n 's/.*"payment_id":"\([^"]*\)".*/\1/p')"
[ -n "$PAYMENT_ID" ] || fail "в ответе нет payment_id: $RESPONSE"
echo "    payment_id=$PAYMENT_ID"

# ждём, пока consumer возьмёт клейм (медленный CI не должен ломать сценарий)
CLAIM_DEADLINE=$(( $(date +%s) + 30 ))
LOCKED_BEFORE="f"
while [ "$(date +%s)" -lt "$CLAIM_DEADLINE" ]; do
  LOCKED_BEFORE="$(psql_value 'locked_until IS NOT NULL')"
  [ "$LOCKED_BEFORE" = "t" ] && break
  sleep 0.5
done
STATUS_BEFORE="$(psql_value 'status')"
echo "==> до SIGKILL: status=$STATUS_BEFORE, клейм занят=$LOCKED_BEFORE"
[ "$STATUS_BEFORE" = "pending" ] || fail "платёж уже финальный, окно обработки пропущено"
[ "$LOCKED_BEFORE" = "t" ] || fail "клейм не взят — consumer не начал обработку"

echo "==> docker kill --signal=KILL $CONSUMER"
docker kill --signal=KILL "$CONSUMER" >/dev/null

sleep 1
LOCKED_AFTER="$(psql_value 'locked_until IS NOT NULL')"
echo "==> после SIGKILL: клейм остался=$LOCKED_AFTER (release_claim не выполнялся)"
[ "$LOCKED_AFTER" = "t" ] || fail "клейм исчез — сценарий не воспроизвёл жёсткое падение"

echo "==> поднимаю consumer заново, жду перехвата клейма (до ${TIMEOUT_SECONDS}s)"
compose -f docker-compose.yml -f "$OVERRIDE" start consumer >/dev/null
CONSUMER="$(compose ps -q consumer)"

DEADLINE=$(( $(date +%s) + TIMEOUT_SECONDS ))
STATUS="pending"
while [ "$(date +%s)" -lt "$DEADLINE" ]; do
  STATUS="$(psql_value 'status')"
  [ "$STATUS" != "pending" ] && break
  sleep 3
done

[ "$STATUS" != "pending" ] || fail "платёж остался pending спустя ${TIMEOUT_SECONDS}s"
CLEARED="$(psql_value 'locked_until IS NULL')"
echo "==> итог: status=$STATUS, клейм снят=$CLEARED"
[ "$CLEARED" = "t" ] || fail "клейм не снят после финализации (locked_until остался)"

echo "==> жду успешную webhook-доставку платежа $PAYMENT_ID"
wait_for_webhook_delivery

DLQ_AFTER="$(dlq_count)" || fail "не удалось проверить DLQ после сценария"
echo "==> $DLQ_QUEUE: было $DLQ_BEFORE, стало $DLQ_AFTER"
[ "$DLQ_AFTER" = "$DLQ_BEFORE" ] \
  || fail "в DLQ добавились сообщения — после обработки осталась неуспешная доставка"

echo "PASS: платёж $PAYMENT_ID дошёл до $STATUS после SIGKILL; клейм снят, webhook доставлен, новых записей в DLQ нет"
