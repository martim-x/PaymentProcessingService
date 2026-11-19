# Payment Processing Service

Асинхронный микросервис процессинга платежей (тестовое задание).

FastAPI принимает платёж → outbox → relay публикует в RabbitMQ (`payments.new`) →
consumer эмулирует шлюз (2–5 сек, 90/10), обновляет статус в PostgreSQL и
отправляет webhook клиенту с подписью HMAC.

Полное описание слоёв и сущностей — [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md).
Схема архитектуры хранится локально (`docs/architecture.excalidraw`) и в репозиторий не входит.

## Стек

- **FastAPI** + Pydantic v2, **SQLAlchemy 2.0 async**, **PostgreSQL**
- **FastStream** (RabbitMQ), **Alembic**, **Docker** + docker-compose
- Слои: domain → application (use cases + ports) → adapters → entrypoints (DDD)

## Быстрый старт

```bash
cp .env.example .env   # замените обязательные API- и webhook-секреты
docker compose up --build
```

Если `.env` отсутствует, Compose использует `local-dev-key` и `local-webhook-secret`.
Это только локальные значения для разработки; не используйте их на публичном стенде.

Поднимаются: postgres, rabbitmq (+UI), api, relay (outbox publisher), consumer;
миграции применяются автоматически.

- API: http://localhost:8000 — Swagger: http://localhost:8000/docs
- RabbitMQ UI: http://localhost:15673 (guest/guest)

## Примеры

### Создание платежа

```bash
curl -X POST http://localhost:8000/api/v1/payments \
  -H "X-API-Key: local-dev-key" \
  -H "Idempotency-Key: order-42" \
  -H "Content-Type: application/json" \
  -d '{
        "amount": "100.50",
        "currency": "RUB",
        "description": "Заказ №42",
        "metadata": {"order_id": "42"},
        "webhook_url": "http://host.docker.internal:9000/webhook"
      }'
```

Ответ — `202 Accepted`:

```json
{"payment_id": "…", "status": "pending", "created_at": "…"}
```

Повтор запроса с тем же `Idempotency-Key` и тем же телом вернёт тот же платёж
(нового платежа не будет); с другим телом — `409 Conflict`.

### Получение платежа

```bash
curl http://localhost:8000/api/v1/payments/<payment_id> -H "X-API-Key: local-dev-key"
```

`status` меняется с `pending` на `succeeded`/`failed` после обработки consumer'ом
(~2–5 сек), затем на `webhook_url` приходит уведомление:

```json
{"status": "succeeded", "id": "…", "amount": "100.5000", "currency": "RUB", "…": "…"}
```

Подпись: заголовок `X-Signature: t=<unix-ts>,v1=<hex>` (плюс `X-Timestamp`).
Сериализацию и HMAC делает сам адаптер `HttpxWebhookSender` — тело и подпись физически
не могут разъехаться.
HMAC-SHA256 считается от `f"{timestamp}.".encode() + body`, где `body` — ровно те байты,
что уходят в сеть (канонический JSON: `sort_keys`, без пробелов).

Проверка на стороне получателя:

```python
from payment.adapters.webhook.signer import verify_body

ok = verify_body(secret, raw_request_body, request.headers["X-Signature"])
```

### Локальный приём webhook

```bash
python3 - <<'PY'
import http.server, json
class H(http.server.BaseHTTPRequestHandler):
    def do_POST(self):
        body = self.rfile.read(int(self.headers['Content-Length']))
        print("webhook:", self.headers.get('X-Signature'), json.loads(body))
        self.send_response(200); self.end_headers()
http.server.HTTPServer(('0.0.0.0', 9000), H).serve_forever()
PY
```

## Надёжность

| Механизм | Реализация |
|---|---|
| **Outbox pattern** | Событие пишется в таблицу `outbox` в той же транзакции, что и платёж; `relay` (поллинг `outbox WHERE sent=false`, `FOR UPDATE SKIP LOCKED`) публикует в `payments.new` и помечает sent |
| **Идемпотентность** | Уникальный `idempotency_key` + fingerprint тела (sha256 канонического JSON, каноническая сумма: `100.5 ≡ 100.50`). Fingerprint считается в use case, а не в HTTP-слое; `IntegrityError` ловится в репозитории и превращается в `IdempotencyViolation`. Повтор → тот же платёж, конфликт → 409 |
| **Retry** | Ошибки consumer'а (БД и т.п.) → очередь `payments.new.retry.N` с TTL `base·factor^(N-1)` и dead-letter назад в `payments.new`; N≤3. Счётчик попыток едет в заголовке `x-attempt` (x-death теряется при перепубликации) |
| **Клейм обработки** | Перед вызовом шлюза: `UPDATE ... SET locked_until = now() + PAYMENT_CLAIM_TTL_SECONDS WHERE status='pending' AND (locked_until IS NULL OR locked_until < now())`. Клейм не даёт двум consumer'ам одновременно обрабатывать платёж, пока он действителен. Защита от повторного списания после истечения клейма/сбоя зависит от поддержки и соблюдения `idempotency_key` внешним шлюзом; fake gateway не моделирует реальные списания. При любом сбое после клейма он **снимается** (`release_claim` по токену). Если клейм занят, а платёж ещё `pending` — бросается `PaymentBusy`, сообщение идёт по retry/DLQ (не ack'ается молча). В шлюз передаётся ключ платежа |
| **Клейм и тайминги** | `PAYMENT_CLAIM_TTL_SECONDS` (30 с) покрывает эмулируемый шлюз (`PAYMENT_PROCESS_MAX_SECONDS`) + финализацию. При реальном шлюзе защита от повторного списания при превышении TTL должна обеспечиваться самим провайдером по `idempotency_key`; fake gateway не моделирует это поведение. Дубликат сообщения ждёт освобождения клейма до `PAYMENT_CLAIM_WAIT_SECONDS` (15 с) и, если платёж стал финальным, тихо завершается — без retry-шума и DLQ |
| **Согласованность клейма и ретраев** | Настройки проверяются на старте: `claim_ttl_seconds ≤ retry_wait_budget`, где бюджет = `max_attempts × claim_wait_seconds + TTL retry-очередей`. Иначе при `kill -9` consumer'а (клейм не снимается) все попытки истекут раньше, чем клейм протухнет, и сообщение уйдёт в DLQ с платежом в `pending`. Проверяется и нижняя граница: `claim_ttl_seconds ≥ PAYMENT_PROCESS_MAX_SECONDS + PAYMENT_CLAIM_FINALIZE_MARGIN_SECONDS` (иначе клейм короче шлюза), и строгий запас: `TTL + 2 × PAYMENT_CLAIM_POLL_SECONDS < бюджет`. С дефолтами TTL 30 с, бюджет 48 с — дубликат перехватывает клейм и доводит платёж до финала |
| **Недоступность брокера** | `PublisherUnavailable` (соединение/канал) **не считается попыткой записи**: батч прерывается, уже опубликованное коммитится, `attempts` не растёт. Иначе рестарт RabbitMQ на 10 секунд «сжёг» бы лимит и все записи ушли бы в `failed` |
| **Poison-запись outbox** | Ошибка публикации одной записи не роняет батч: у записи растёт `attempts`, после `PAYMENT_OUTBOX_MAX_ATTEMPTS` она помечается `failed` и не мешает остальным |
| **DLQ** | После 3 попыток сообщение уходит в `payments.dlq` (и из main по `x-dead-letter`) |
| **Webhook retry** | Попытки с экспоненциальной задержкой (`ExponentialBackoffRetryPolicy`), статус платежа уже закоммичен и от webhook не зависит |

## Безопасность

- **Секреты обязательны вне локального Compose**: приложение отвергает пустые и известные
  заглушечные значения `PAYMENT_API_KEY` / `PAYMENT_WEBHOOK_SECRET`. Compose без `.env`
  подставляет только локальные значения для разработки (см. быстрый старт); их нельзя
  использовать на публичном стенде.
- **Сравнение API-ключа** — `hmac.compare_digest` (константное время).
- **SSRF-гард**: `webhook_url` проверяется перед отправкой — блокируются loopback,
  приватные, link-local и зарезервированные адреса (после DNS-резолва).
  Для локальных стендов есть `PAYMENT_WEBHOOK_ALLOW_PRIVATE_HOSTS=true`.
- **Валидация**: ошибки домена (`InvalidMoney`, `InvalidWebhookUrl`) → `422`, а не `500`;
  лимиты на `description` (1000) и `metadata` (8 KB), сумма ≤ `Numeric(19,4)`.
- **Fingerprint** считается от канонической суммы (`100.5` ≡ `100.50`) — без ложных 409.
- **Webhook-payload компактный**: без `metadata` и собственного `webhook_url`.

## Воспроизводимые проверки

| Сценарий | Как запустить | Что проверяет |
|---|---|---|
| SIGKILL consumer'а посреди обработки | `./scripts/chaos_kill9.sh` | клейм остаётся после `docker kill --signal=KILL`, дубликат дожидается протухания TTL, перехватывает клейм и доводит платёж до финала; клейм снят, новых записей в DLQ нет (сравнение с числом до сценария) |
| Полный поток API → outbox → consumer → webhook | `pytest tests/e2e` | 202, идемпотентный повтор, 409, смена статуса, HMAC-подпись от сырых байтов |
| Outbox против реального PostgreSQL | `pytest tests/integration` | атомарность UoW, claim/finalize/release, poison-записи |

`scripts/chaos_kill9.sh` выполняется и в CI (job **E2E**), поэтому гарантия
«платёж не застревает после жёсткого падения» подтверждается автоматически, а не словами.

## Ограничения (осознанные)

- **Доставка webhook не идёт через outbox.** Если consumer упадёт между commit'ом статуса
  и отправкой, уведомление потеряется (статус при этом корректен). Для гарантии нужен
  отдельный outbox webhook-событий с собственным retry — не реализовано.
- **Отправленные строки outbox не чистятся**: индекс `ix_outbox_unsent` частичный
  (`WHERE sent = false`), но сама таблица растёт — нужна периодическая архивация.
- **SSRF-проверка при отправке идёт отдельным DNS-запросом**, поэтому теоретически
  возможен DNS-rebinding между проверкой и коннектом httpx (для полной защиты нужен
  кастомный transport с подключением по проверенному IP). Литеральные приватные IP
  отсекаются ещё на API (`422`).
- **Relay читает outbox с `FOR UPDATE SKIP LOCKED`** на время публикации батча —
  несколько relay'ев безопасны; «ядовитая» запись ограничена `PAYMENT_OUTBOX_MAX_ATTEMPTS`.
- **`ProcessPayment` — толстый use case**: вызов шлюза, финализация, payload и webhook-ретраи
  в одном сценарии. Доставка уведомления заслуживает отдельного `NotifyWebhook` со своим
  outbox — не реализовано (см. пункт про гарантию доставки).
- **Политика retry/DLQ** вынесена в адаптер `adapters/messaging/routing.py`;
  entrypoint только вызывает её.
- **Миграция `0001_initial` правилась на месте** (в неё входят `locked_until`,
  `failed`, `attempts`). Для уже применённой схемы Alembic её не перенакатит —
  нужен `docker compose down -v` (чистый стенд) или отдельная ревизия.
- **Занятый клейм**: дубликат ждёт до `PAYMENT_CLAIM_WAIT_SECONDS`; если клейм так
  и не освободился (первый consumer завис или шлюз медленнее TTL), сообщение уходит
  в retry/DLQ — это корректное поведение, DLQ разбирается вручную.
- **Отмена задачи (SIGTERM)** теперь тоже снимает клейм: `except BaseException`
  + `asyncio.shield`, поэтому `CancelledError` не оставляет платёж заблокированным.

## Разработка

Проект требует Python ≥ 3.12; **локально весь набор тестов и линтеров прогнан на
Python 3.15.2** (venv проекта), в CI используется 3.13.

```bash
python3.13 -m venv venv
venv/bin/pip install -e ".[test]" ruff mypy bandit pre-commit
```

### Гейты качества (те же, что в CI)

```bash
venv/bin/ruff check src tests                            # линтер
venv/bin/ruff format --check src tests
venv/bin/python -m mypy src/payment                      # типы
venv/bin/python -m bandit -r -c pyproject.toml -q src    # безопасность
venv/bin/python -m pytest tests/unit -q                  # unit
venv/bin/python -m pytest tests/integration -q           # integration (нужен PG)
venv/bin/python -m pytest tests/e2e -q                   # e2e (нужен живой compose)
```

### Pre-commit

```bash
pre-commit install   # ruff, bandit, mypy, unit-тесты на каждый коммит
```

### CI

GitHub Actions (`.github/workflows/ci.yml`) — 6 job'ов:
**ruff → mypy → bandit → unit → integration (PG-сервис) → e2e (docker compose)**.

**Время жизни зависимостей:** use case'ы создаются на каждый запрос
(`Depends`-фабрика), UoW хранит сессию и защищён от повторного входа —
иначе параллельные запросы смешали бы транзакции.

Consumer берёт `PAYMENT_CONSUMER_PREFETCH` сообщений одновременно; webhook-ретраи
выполняются внутри обработчика и занимают слот — при большом потоке поднимайте
и prefetch, и число consumer-реплик.

Интеграционные тесты требуют PostgreSQL:

```bash
PAYMENT_DATABASE_URL=postgresql+psycopg://payment:payment@localhost:5433/payments \
  venv/bin/python -m pytest tests/integration -q
```

E2E-тесты требуют поднятый compose (`docker compose up -d`) и слушают порт 9000
для приёма webhook; адрес хоста из контейнера — `host.docker.internal`.
E2E проверяет и саму HMAC-подпись: считает её заново от сырых байтов полученного тела.

## Структура

```
src/payment/
├── domain/          # агрегат Payment, value objects, события (не знает о фреймворках)
├── application/     # use cases (create/process/get/publish_outbox) + порты
├── adapters/        # persistence (SQLAlchemy), messaging (FastStream), gateway, webhook
├── entrypoints/     # api (FastAPI), consumer (FastStream), relay (поллинг outbox)
└── bootstrap.py     # composition root

tests/
├── unit/            # домен + use cases на fake-портах
├── integration/     # репозитории, UoW, outbox на реальном PostgreSQL
└── e2e/             # API → outbox → consumer → webhook
```
