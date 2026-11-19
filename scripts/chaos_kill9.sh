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

API_URL="${PAYMENT_API_URL:-http://localhost:8000}"
API_KEY="${PAYMENT_API_KEY:-local-dev-key}"
GATEWAY_SECONDS="${CHAOS_GATEWAY_SECONDS:-20}"
TIMEOUT_SECONDS="${CHAOS_TIMEOUT_SECONDS:-120}"

OVERRIDE="$(mktemp -t chaos-kill9-override.XXXXXX.yml)"
CONSUMER="payment-service-consumer-1"

cat >"$OVERRIDE" <<YAML
services:
  consumer:
    environment:
      PAYMENT_PROCESS_MIN_SECONDS: "${GATEWAY_SECONDS}"
      PAYMENT_PROCESS_MAX_SECONDS: "${GATEWAY_SECONDS}"
YAML

restore_consumer() {
  echo "==> восстанавливаю consumer в штатной конфигурации"
  docker compose -f docker-compose.yml -f "$OVERRIDE" down consumer >/dev/null 2>&1 || true
  docker compose up -d --no-deps consumer >/dev/null 2>&1 || true
  rm -f "$OVERRIDE"
}
trap restore_consumer EXIT

fail() {
  echo "FAIL: $*" >&2
  docker compose logs --tail 40 consumer >&2 || true
  exit 1
}

psql_value() {
  # $1 — SQL-выражение, возвращающее скаляр
  docker exec payment-service-postgres-1 psql -U payment -d payments -t -A \
    -c "SELECT $1 FROM payments WHERE id = '$PAYMENT_ID';"
}

echo "==> consumer с медленным шлюзом (${GATEWAY_SECONDS}s)"
docker compose -f docker-compose.yml -f "$OVERRIDE" up -d --no-deps consumer >/dev/null
sleep 4

echo "==> создаю платёж"
RESPONSE="$(curl -sf -X POST "$API_URL/api/v1/payments" \
  -H "X-API-Key: $API_KEY" \
  -H "Idempotency-Key: chaos-kill9-$(date +%s)" \
  -H "Content-Type: application/json" \
  -d '{"amount":"13.37","currency":"RUB","description":"chaos kill -9",
       "metadata":{"check":"chaos"},"webhook_url":"http://host.docker.internal:9000/webhook"}')" \
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
eLOST="$(psql_value 'locked_until IS NOT NULL')"
echo "==> после SIGKILL: клейм остался=$eLOST (release_claim не выполнялся)"
[ "$eLOST" = "t" ] || fail "клейм исчез — сценарий не воспроизвёл жёсткое падение"

echo "==> поднимаю consumer заново, жду перехвата клейма (до ${TIMEOUT_SECONDS}s)"
docker compose -f docker-compose.yml -f "$OVERRIDE" start consumer >/dev/null

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

DLQ="$(docker exec payment-service-rabbitmq-1 rabbitmqctl list_queues name messages 2>/dev/null \
  | awk '$1 == "payments.dlq" {print $2}')"
echo "==> payments.dlq messages=${DLQ:-0}"
[ "${DLQ:-0}" = "0" ] || fail "сообщение ушло в DLQ — перехват клейма не сработал"

echo "PASS: платёж $PAYMENT_ID дошёл до $STATUS после SIGKILL, DLQ пуст"
