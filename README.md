# Payment Processing Service

Асинхронный микросервис процессинга платежей (тестовое задание).

FastAPI принимает платёж → outbox → relay публикует в RabbitMQ (`payments.new`) →
consumer эмулирует шлюз (2–5 сек, 90/10), обновляет статус в PostgreSQL и
отправляет webhook клиенту с подписью HMAC.

Полное описание слоёв и сущностей — [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md).
(Схема архитектуры хранится локально в `docs/architecture.excalidraw` / `docs/architecture.png`
и намеренно не публикуется в репозитории.)

## Стек

- **FastAPI** + Pydantic v2, **SQLAlchemy 2.0 async**, **PostgreSQL**
- **FastStream** (RabbitMQ), **Alembic**, **Docker** + docker-compose
- Слои: domain → application (use cases + ports) → adapters → entrypoints (DDD)

## Быстрый старт

```bash
cp .env.example .env   # при необходимости поправьте значения
docker compose up --build
```

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

Заголовок `X-Signature: sha256=<hex>` — HMAC-SHA256 тела от секрета
`PAYMENT_WEBHOOK_SECRET` (проверка: `payment.adapters.webhook.signer.verify_payload`).

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
| **Идемпотентность** | Уникальный `idempotency_key` + fingerprint тела (sha256 канонического JSON): повтор → тот же платёж, конфликт → 409; consumer устойчив к дублям (at-least-once) |
| **Retry** | Ошибки consumer'а (БД и т.п.) → очередь `payments.new.retry.N` с TTL `base·factor^(N-1)` и dead-letter назад в `payments.new`; N≤3 |
| **DLQ** | После 3 попыток сообщение уходит в `payments.dlq` (и из main по `x-dead-letter`) |
| **Webhook retry** | Попытки с экспоненциальной задержкой (`ExponentialBackoffRetryPolicy`), статус платежа уже закоммичен и от webhook не зависит |

## Разработка

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

Integration-тесты требуют PostgreSQL:

```bash
PAYMENT_DATABASE_URL=postgresql+psycopg://payment:payment@localhost:5433/payments \
  venv/bin/python -m pytest tests/integration -q
```

E2E-тесты требуют поднятый compose (`docker compose up -d`) и слушают порт 9000
для приёма webhook; адрес хоста из контейнера — `host.docker.internal`.

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
