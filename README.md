# Payment Processing Service

Реализация тестового задания: асинхронный сервис обработки платежей на FastAPI, PostgreSQL и RabbitMQ. Сервис принимает запрос, сохраняет платёж и событие outbox, обрабатывает платёж в фоне и отправляет результат на webhook. Внешний платёжный шлюз эмулируется.

## Цель и требования

Сервис принимает запросы на оплату, обрабатывает их в фоне и сообщает результат через webhook.

| Пункт задания | Где реализовано |
|---|---|
| Модели и миграции `payments` + `outbox` | `src/payment/adapters/persistence/orm.py`, `migrations/versions/` |
| API: создание/чтение, `X-API-Key`, `Idempotency-Key` | `src/payment/entrypoints/api/` |
| Эмуляция шлюза 2–5 с, 90/10 | `src/payment/adapters/gateway/fake_gateway.py` |
| Consumer: один обработчик очереди `payments.new`, последовательно выполняющий обработку платежа и webhook | `entrypoints/consumer/app.py`, `application/services/payment_service.py`, `application/services/webhook_service.py` |
| Outbox pattern | `application/services/payment_service.py`, `application/services/outbox_service.py`, `entrypoints/relay/runner.py` |
| TTL-retry и DLQ | `adapters/messaging/topology.py` (общий бюджет отказов, две лестницы задержек), `routing.py` |
| DLQ | `payments.dlq` — durable-очередь; объявляется consumer'ом и relay, разбирается вручную |
| docker-compose | `docker-compose.yml` |
| README + примеры | этот файл, [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) |

Runtime запускает consumer-процесс с единственной активной подпиской `payments.new`: один обработчик вызывает `PaymentService`, затем `WebhookService.deliver`, и только после обоих шагов подтверждает сообщение. Отдельного webhook-конвейера нет — ни очереди, ни второго consumer'а.

## Архитектура и паттерны

Код разделён на доменную модель, прикладные сценарии, адаптеры и точки входа. Используются элементы DDD и Ports & Adapters.

- **Domain** (`src/payment/domain/`): `Payment` — сущность и корень агрегата, управляющий переходами `pending → succeeded/failed`. `Money`, `PaymentId`, `IdempotencyKey`, `WebhookUrl` — Value Objects. `PaymentCreated`, `PaymentSucceeded`, `PaymentFailed` — доменные события. `ExponentialBackoffRetryPolicy` — Policy.
- **Application** (`src/payment/application/`): сервисы сценариев (`services/`) координируют шаги через UoW и порты; порты задают контракты репозиториев, Unit of Work, шлюза, webhook sender и publisher.
- **Adapters** (`src/payment/adapters/`): реализации портов для SQLAlchemy/PostgreSQL, FastStream/RabbitMQ, fake gateway и HTTP webhook.
- **Entrypoints** (`src/payment/entrypoints/`): FastAPI, единый consumer платежа и webhook, relay (публикация outbox).
- **Composition root** (`src/payment/bootstrap.py`): собирает адаптеры и фабрики сервисов. Домен не зависит от FastAPI, SQLAlchemy или FastStream.

Доменное событие описывает факт бизнес-модели; сообщение RabbitMQ — транспортный способ доставки, не то же самое.

```text
Entrypoints (API / Consumer / Relay)
                 ↓
        Application (Services)
           ↓             ↑
        Domain       Ports (контракты)
                          ↑
        Adapters: PostgreSQL / RabbitMQ / HTTP
```

Подробная карта файлов, потоков и архитектурных решений приведена в [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md).

## Технические решения

- **Transactional Outbox + Unit of Work.** Платёж и событие outbox записываются одной транзакцией PostgreSQL. Финальный статус и событие `payment.webhook` — тоже одна транзакция: если вставка в outbox падает, статус откатывается. Relay поллит таблицу (`FOR UPDATE SKIP LOCKED`), маршрутизирует запись по `event_type` и публикует с `persist`, `mandatory` и publisher confirms. Запись `payment.webhook` (payload для возобновления доставки) сохраняется с `dispatch='local'`: relay её не читает, а `sent` остаётся честным флагом публикации и не выставляется заранее. Фактический статус доставки ведётся в `webhook_deliveries`.
- **Идемпотентность API.** Уникальный `idempotency_key` и SHA-256 fingerprint канонического тела (сумма нормализуется: `100.5 ≡ 100.50`). Тот же запрос возвращает существующий платёж; тот же ключ с другим телом — `409 Conflict`. Гонку двух одновременных POST разрешает unique-индекс: проигравший ловит `IntegrityError` и перечитывает платёж победителя.
- **Клейм обработки платежа.** Атомарный `UPDATE ... SET locked_until = now() + ttl WHERE status='pending' AND (locked_until IS NULL OR locked_until < now())` не допускает конкурентную обработку. Шлюз и финализация идут вне транзакции; финальный статус пишется условным `UPDATE` под токеном клейма. Клейм снимается при любой ошибке и при `SIGTERM`; при `SIGKILL` он истекает по TTL. Дубликат сообщения ждёт освобождения клейма до `PAYMENT_CLAIM_WAIT_SECONDS` и тихо завершается, если платёж уже финальный.
- **Клейм доставки webhook.** Таблица `webhook_deliveries` хранит состояние доставки по стабильному `event_id`; `locked_until` работает как fencing token — записать итог может только владелец действующего клейма. Устаревший обработчик не перетирает состояние нового.
- **Retry/DLQ без `sleep`.** Активный обработчик `payments.new` охватывает обработку платежа и доставку webhook; при сбоях повторно маршрутизируется исходное сообщение через TTL-очередь и DLQ. Бюджет отказов общий: `max_attempts` считается по сумме `x-attempt` и `x-infra-attempt`, поэтому третья неудача отправляет сообщение в DLQ независимо от того, чередовались ли бизнес- и инфраструктурные сбои. `PaymentBusy` и `WebhookBusy` откладываются без расхода бюджета. Сбой публикации retry/DLQ возвращает исходное сообщение в очередь (`nack requeue`), сохраняя его для следующей попытки. Повторное помещение из DLQ — ручная операция: см. раздел «Ручной разбор DLQ».
- **Инфраструктурные ретраи.** Транзиентный сбой БД/транспорта уходит в отдельную лестницу задержек `payments.new.infra.retry.N` (счётчик `x-infra-attempt`, база `PAYMENT_INFRA_RETRY_BASE_DELAY`): паузы длиннее, чем у обычных ретраев. Бюджет отказов при этом общий — `x-attempt` и `x-infra-attempt` складываются и сравниваются с `PAYMENT_MAX_ATTEMPTS`.
- **Webhook.** HMAC-SHA256 от `timestamp` и точных байтов тела в `X-Signature` (`t=<unix-ts>,v1=<hex>`); сериализация и подпись живут в одном адаптере, поэтому тело и подпись не могут разъехаться. URL проверяется на SSRF (литеральные приватные IP — ещё на API, `422`).

- **Недоступность брокера**. `PublisherUnavailable` (соединение/канал/нет confirms) **не считается попыткой записи**: батч прерывается, уже опубликованное коммитится, `attempts` не растёт. Иначе рестарт RabbitMQ «сжёг» бы лимит и записи ушли бы в `failed`
- **Poison-запись outbox**. Ошибка публикации одной записи не роняет батч: у неё растёт `attempts`, после `PAYMENT_OUTBOX_MAX_ATTEMPTS` она помечается `failed` и не блокирует остальные
- **Poison-сообщение**. Невалидный JSON или `payment_id` сразу уходит в `payments.dlq`, без бесконечного ретрая
- **Согласованность таймингов**. На старте проверяется `claim_ttl ≥ process_max + margin`, `claim_ttl + 2·poll < retry_wait_budget` (с дефолтами 31 с < 48 с), `webhook_claim_ttl ≥ webhook_timeout + margin`

## Стек

- **FastAPI** + Pydantic v2, **SQLAlchemy 2.0 async**, **PostgreSQL**
- **FastStream** (RabbitMQ), **Alembic**, **Docker** + docker-compose
- Слои: domain → application (services + ports) → adapters → entrypoints (DDD)

## Быстрый старт

Задайте один раз в текущем Bash ключ API и webhook-секрет. Те же переменные Compose передаст сервисам, а команды ниже используют их напрямую; `.env` не требуется.

```bash
read -rsp 'API key: ' PAYMENT_API_KEY; printf '\n'
read -rsp 'Webhook secret: ' PAYMENT_WEBHOOK_SECRET; printf '\n'
export PAYMENT_API_KEY PAYMENT_WEBHOOK_SECRET
export PAYMENT_WEBHOOK_ALLOW_PRIVATE_HOSTS=true
docker compose up --build -d
```

Приложение отвергает пустые и известные заглушечные значения. `host.docker.internal` для локального приёмника разрешён только с `PAYMENT_WEBHOOK_ALLOW_PRIVATE_HOSTS=true`.

Поднимаются сервисы: `postgres` (5433 → 5432), `rabbitmq` (5673, UI 15673, данные в
volume `rabbitmqdata44`), одноразовый `migrate` (`alembic upgrade head`), `api`, `relay` и
один `consumer`. Миграции применяются автоматически, остальные сервисы ждут их
завершения.

Используются PostgreSQL 19.0 и RabbitMQ 4.4.1. Для них выделены новые volumes
`pgdata19` и `rabbitmqdata44`; прежние `payment-service_pgdata` (PG16) и
`payment-service_rabbitmqdata` не подключаются и не изменяются. Данные автоматически
между major-версиями не мигрируют: для сохранения БД требуется отдельная проверенная
миграция/backup-restore, RabbitMQ также требует отдельной процедуры совместимого
обновления. Не удаляйте старые volumes, пока данные не перенесены и не проверены.

- API: http://localhost:8000. Swagger UI, ReDoc и публичный `/openapi.json` отключены — ТЗ требует `X-API-Key` на всех эндпоинтах. Схема доступна под ключом на `/api/v1/openapi.json`, порядок работы — в разделе «Документация API». `/health` тоже требует ключ.
- RabbitMQ UI: http://localhost:15673 (guest/guest)

## Примеры

### Создание платежа

Перед POST запустите «Локальный приём webhook» в отдельном терминале.

```bash
curl -i -X POST http://localhost:8000/api/v1/payments \
  -H "X-API-Key: $PAYMENT_API_KEY" \
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

Ответ `202 Accepted` содержит `payment_id`, `status`, `created_at`; новый платёж также возвращает `Location`. Получить его можно так:

```bash
read -rp 'payment_id из ответа POST: ' PAYMENT_ID
curl -H "X-API-Key: $PAYMENT_API_KEY" \
  "http://localhost:8000/api/v1/payments/$PAYMENT_ID"
```

Повтор с тем же ключом и телом возвращает существующий платёж; другое тело с тем же ключом даёт `409`.

### Webhook

На `webhook_url` приходит компактное уведомление (ключи отсортированы — так его
сериализует подписывающий адаптер):

```json
{
  "amount": "100.5000",
  "currency": "RUB",
  "event": "payment.succeeded",
  "event_id": "9f2c…",
  "failure_reason": null,
  "id": "…",
  "processed_at": "2026-11-19T09:15:03.123456+00:00",
  "status": "succeeded"
}
```

`metadata` и собственный `webhook_url` в тело не попадают. `event_id` — идентификатор
события доставки: он одинаков во всех попытках доставки одного уведомления, по нему
получатель дедуплицирует повторы.

Подпись: `X-Signature: t=<unix-ts>,v1=<hex>` и `X-Timestamp`. HMAC-SHA256 считается от
`f"{timestamp}." + body`, где `body` — ровно те байты, что ушли в сеть (канонический
JSON: `sort_keys`, без пробелов).

Проверка на стороне получателя:

```python
from payment.adapters.webhook.signer import WebhookSigner

ok = WebhookSigner.verify_body(secret, raw_request_body, request.headers["X-Signature"])
```

`WebhookSigner.verify_body` дополнительно проверяет окно по времени (по умолчанию 300 секунд).

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

## Документация API

Swagger UI, ReDoc и публичный `/openapi.json` отключены: ТЗ требует `X-API-Key` на
всех эндпоинтах, а FastAPI регистрирует страницы документации без авторизации.
Схема осталась доступной — но только под тем же ключом:

```bash
curl -fsS -H "X-API-Key: $PAYMENT_API_KEY" \
  http://localhost:8000/api/v1/openapi.json -o openapi.json
```

Дальше её смотрит любой внешний инструмент: `npx @redocly/cli preview-docs openapi.json`,
редактор `editor.swagger.io`, Postman или Insomnia. Публичных страниц, отвечающих без
ключа, в приложении нет: `/docs`, `/redoc` и `/openapi.json` отдают `404`.

## Ручной разбор DLQ

В `payments.dlq` лежит конверт `payment.created` с `payment_id`; тело HTTP-уведомления
хранится в БД. Просто вернуть сообщение в очередь недостаточно: запись
`webhook_deliveries` может остаться в `failed`, и consumer снова завершит сообщение
через DLQ, не отправив ни одного запроса.

1. Найти платёж и состояние доставки:

   ```bash
   docker compose exec postgres psql -U payment -d payments -c \
     "select event_id, payment_id, status, attempts, last_error
        from webhook_deliveries order by updated_at desc limit 5;"
   ```

2. Сбросить бюджет доставки (только после того, как причина неудачи устранена):

   ```sql
   update webhook_deliveries
      set status = 'pending', attempts = 0, last_error = null, locked_until = null
    where event_id = '<event_id>';
   ```

3. **Не используйте `ack_requeue_false` для redrive.** Этот режим уничтожает
   сообщение из DLQ до того, как отдельная публикация завершится; если publish
   не удался, исходник уже потерян. Management API `get` не предоставляет
   атомарного transfer DLQ → рабочая очередь. Для ручного redrive с сохранением
   исходника используйте RabbitMQ UI «Get messages» с `Ack Mode = reject_requeue_true`
   (оставить/вернуть исходник в DLQ), затем скопируйте полный payload и свойства
   сообщения и опубликуйте копию в `payments.new`. В Management API при публикации
   считайте операцию
   успешной только при `routed: true`; HTTP 200 без `routed: true` не подтверждает,
   что сообщение дошло до очереди. Пример для публикации (подставьте полный payload,
   корректно JSON-экранированный):

   ```json
   {"properties":{"delivery_mode":2},"routing_key":"payments.new","payload":"<полный исходный payload>","payload_encoding":"string"}
   ```

   Исходник оставьте в DLQ до проверки результата: обычный `get` забирает сообщение
   с начала очереди и не умеет безопасно выбрать нужное по `payment_id`. Удаление
   отдельных сообщений из общей DLQ этой инструкцией не автоматизировано; для него
   нужен отдельный consumer с ручным ACK и выбором нужного сообщения. Между публикацией
   копии и удалением исходника нет общей транзакции, поэтому возможны дубликаты.
   Сохраняйте исходный payload и свойства, если они есть; получатель webhook должен
   дедуплицировать уведомления по стабильному `event_id`. Проверьте статус доставки.

## Семантика доставки: at-least-once

Сообщения и webhook могут повторяться (at-least-once, не exactly-once). Это не обещание успешной HTTP-доставки: после исчерпания попыток сообщение уходит в DLQ. Повторы обрабатываются так:

- API — `Idempotency-Key` + fingerprint тела;
- шлюз — id платежа передаётся как `idempotency_key` (fake gateway ничего не списывает;
  реальный провайдер должен сам поддержать ключ);
- брокер — клейм платежа и клейм доставки с fencing token;
- получатель webhook — `event_id`.

Повторное уведомление возможно, если процесс упал между успешным HTTP-запросом и
записью `delivered` (в том числе `SIGKILL`), если клейм истёк во время отправки и
событие подхватил другой consumer, либо при ручной переигровке сообщения из DLQ.
`event_id` при этом не меняется, поэтому повтор распознаётся получателем.

## Безопасность

- **Секреты обязательны.** Приложение отвергает пустые и известные заглушечные значения
  `PAYMENT_API_KEY` / `PAYMENT_WEBHOOK_SECRET` и не стартует. `.env.example` — шаблон:
  placeholders нужно заменить (см. быстрый старт).
- **Сравнение API-ключа** — `hmac.compare_digest` (константное время).
- **SSRF-гард.** `webhook_url` проверяется перед отправкой: блокируются loopback,
  приватные, link-local, зарезервированные адреса и CGNAT — после DNS-резолва.
  Литеральные приватные IP отсекаются ещё на API (`422`). Для локальных стендов есть
  `PAYMENT_WEBHOOK_ALLOW_PRIVATE_HOSTS=true`; в Compose по умолчанию разрешение выключено.
- **Валидация входа.** `description` ≤ 1000 символов, `metadata` ≤ 8 KB и только
  конечные числа/строки без NUL, `webhook_url` ≤ 2048, сумма > 0 и в пределах
  `Numeric(19,4)`. Доменные ошибки (`InvalidMoney`, `InvalidWebhookUrl`) → `422`, не `500`.
- **Webhook-payload компактный**: без `metadata` и собственного `webhook_url`.
- **Подпись** HMAC-SHA256 с timestamp и окном проверки — защита от подмены тела и replay.
- **Контейнер** работает не от root (uid 10001).

### Ограничения безопасности

Ограничения текущей реализации:

- **Статический API-ключ**: один на весь сервис, без ротации, скоупов, арендаторов и
  rate limiting. Утечка ключа = полный доступ к API.

- **Нет TLS**: ни HTTP API, ни AMQP, ни webhook-вызовы не терминируют TLS. Рассчитано
  на reverse-proxy/доверенную сеть; в Compose порты опубликованы на хост.
- **Один общий webhook-секрет** для всех получателей; нет per-endpoint секретов и
  ротации. Получатель обязан сам проверять окно timestamp (библиотечный
  `WebhookSigner.verify_body` это делает, но только если его вызвать).
- **SSRF-защита не абсолютна**: проверка идёт отдельным DNS-запросом, поэтому
  теоретически возможен DNS-rebinding между проверкой и коннектом `httpx`. Для локального
  E2E private targets разрешайте явно через `PAYMENT_WEBHOOK_ALLOW_PRIVATE_HOSTS=true`.
- **Секреты только в переменных окружения** (`.env`, compose environment): нет vault/KMS,
  значения видны в `docker inspect`. `.env` в git не попадает, `.env.example` — только
  шаблон.
- **Инфраструктура без изоляции**: RabbitMQ `guest/guest` и management UI, PostgreSQL с
  паролем из compose опубликованы на хост; отдельного сетевого периметра нет.
- **Данные не шифруются at-rest**: `outbox`, `webhook_deliveries`, `payments` лежат в
  открытом виде. `metadata` — произвольный JSON, сервис его не чистит и не маскирует.
- **Нет аудита доступа и антифрода**: нет журнала, кто и когда читал платёж; нет лимитов
  на количество платежей и на размер тела запроса целиком (ограничены только поля).
- **DLQ и ручные операции** не аутентифицированы: доступ к RabbitMQ UI = доступ к
  сообщениям и возможность переигровки.

## Воспроизводимые проверки

| Сценарий | Как запустить | Что проверяет |
|---|---|---|
| Три неудачных webhook → DLQ | см. блок ниже: нужны `PAYMENT_RUN_DLQ_E2E=1` и адрес management API (в CI — отдельный шаг job'а **E2E**) | ровно 3 HTTP-попытки с одинаковым `event_id`, событие в `payments.dlq` |
| SIGKILL consumer'а посреди обработки | `./scripts/chaos_kill9.sh` | клейм остаётся после `docker kill --signal=KILL`, дубликат дожидается TTL, перехватывает клейм и доводит платёж до финала; клейм снят, новых записей в `payments.dlq` нет |
| Полный поток API → outbox → consumer → webhook | `pytest tests/e2e` | 202 + `Location`, идемпотентный повтор, 409, смена статуса, HMAC-подпись от сырых байтов, отсутствие `metadata`/`webhook_url` в теле, изоляция от чужих уведомлений на общем порту |
| Outbox, UoW, клеймы на реальном PostgreSQL | `pytest tests/integration` | атомарность платежа и outbox, claim/finalize/release, истечение клейма, `fetch_unsent` |
| Инварианты конфигурации и топологии | `pytest tests/unit` | бюджеты клеймов, TTL retry-очередей, prefetch, маршрутизация outbox, fencing, паритет ORM-`metadata` с миграциями |

```bash
# Выполняйте в том же Bash: тест наследует ключ API и webhook-секрет.
# Остановите ручной приёмник: тест сам займёт порт 9000.
PAYMENT_RUN_DLQ_E2E=1 \
PAYMENT_API_URL=http://localhost:8000 \
PAYMENT_RABBITMQ_MANAGEMENT_URL=http://localhost:15673 \
PAYMENT_RABBITMQ_MANAGEMENT_API_KEY=guest:guest \
PAYMENT_E2E_WEBHOOK_PORT=9000 \
  venv/bin/python -m pytest tests/e2e/test_failed_webhook_dlq.py -q
```

Флаг `PAYMENT_WEBHOOK_ALLOW_PRIVATE_HOSTS=true` нужен не только первому `docker
compose up`: каждый `up`/`start` пересоздаёт consumer и подставляет значение заново
из окружения. Если флаг потерян, SSRF-гард отклоняет `host.docker.internal`
(`private address 172.17.0.1`), и webhook-сценарии видят 0 HTTP-запросов.
`scripts/chaos_kill9.sh` поэтому запоминает флаг у работающего consumer'а и
восстанавливает его при пересоздании.

`scripts/chaos_kill9.sh` выполняется и в CI (job **E2E**). Он проверяет конкретный
сценарий: consumer получает claim, завершается через `SIGKILL` во время эмуляции
шлюза, затем после истечения claim платёж финализируется, webhook получает `2xx`,
а DLQ не увеличивается. Это не доказывает восстановление при любом типе сбоя или
для реального шлюза. Скрипт поднимает локальный webhook sink на порту
`CHAOS_WEBHOOK_PORT` (по умолчанию `9001`); `COMPOSE_PROJECT_NAME` позволяет выбрать
Compose-проект явно.

## Ограничения (осознанные)

- **Отправленные строки outbox и записи `webhook_deliveries` не чистятся.** Индексы
  `ix_outbox_unsent` (`WHERE sent = false`) и `ix_outbox_relay_pending` (`WHERE sent =
  false AND failed = false AND dispatch = 'relay'`) частичные, но таблицы растут —
  нужна периодическая архивация.
- **Relay держит блокировки строк на время публикации батча.** `FOR UPDATE SKIP LOCKED`
  берётся при выборке, а `commit` — после публикации всего батча, поэтому сетевые
  вызовы в брокер происходят внутри открытой транзакции. Несколько relay'ев безопасны
  (пропускают заблокированные строки), но при медленном брокере транзакция и блокировки
  живут дольше. Вынос публикации за транзакцию потребовал бы отдельного статуса
  «publishing» и reclaim-логики.
- **`webhook_deliveries.attempts` и заголовки очереди расходятся.** Заголовки считают отказы обработки сообщения (включая сбои БД), `webhook_deliveries.attempts` — только зафиксированные неудачи HTTP-доставки. Поэтому событие может попасть в DLQ раньше, чем в `webhook_deliveries` накопится три неудачи: это ожидаемо и разбирается вручную.
- **Ручной DLQ replay требует сброса записи доставки**: возврат сообщения в очередь сам по себе не снимает `failed` в `webhook_deliveries`, и consumer снова завершит сообщение через DLQ, не отправив HTTP-запрос. Пошаговая процедура — в разделе «Ручной разбор DLQ».
- **`event_id` не бизнес-ключ.** Он генерируется при финализации и сохраняется для повторов уведомления.
- **Повторные уведомления возможны** (at-least-once): получатель обязан дедуплицировать
  по `event_id`. Гарантии «ровно одно уведомление» нет.
- **Клейм платежа не защищает от двойного списания при истечении TTL.** Пока клейм
  жив, второй consumer не начнёт обработку; если реальный шлюз медленнее TTL, защита
  ложится на `idempotency_key` провайдера. Fake gateway списаний не моделирует.
- **SSRF-проверка при отправке идёт отдельным DNS-запросом** — окно для DNS-rebinding
  (для полной защиты нужен transport с подключением по проверенному IP).
- **Миграция `0001_initial` правилась на месте** (в неё входят `locked_until`, `failed`,
  `attempts`, `event_type`). Для уже применённой схемы Alembic её не перенакатит:
  нужен `docker compose down -v` (чистый стенд) или отдельная ревизия. `0002` добавляет
  `webhook_deliveries` и индекс `ix_outbox_event_type`; `0003` — колонку
  `outbox.dispatch` и частичный индекс `ix_outbox_relay_pending`.
- **Зависший клейм**: если платёж так и не освободился (первый consumer завис, шлюз
  медленнее TTL), сообщение уходит в retry/DLQ — это корректное поведение, DLQ
  разбирается вручную.
- **Отмена задачи (SIGTERM)** снимает клеймы платежа и доставки под их токенами
  (`asyncio.shield`), поэтому `CancelledError` не оставляет запись заблокированной.

## Разработка

Проект требует Python ≥ 3.15.2; в CI используется Python 3.15.2.

```bash
python3.15 -m venv venv
venv/bin/pip install --require-hashes -r requirements.lock
venv/bin/pip install --no-deps -e .
venv/bin/pip install ruff==0.17.5 black==26.5.1 mypy==2.4.1 'bandit[toml]==1.9.4' pre-commit
```

### Гейты качества (те же, что в CI)

```bash
venv/bin/ruff check src tests                            # линтер
venv/bin/python -m black --check src tests migrations scripts
venv/bin/python -m mypy src/payment                      # типы
venv/bin/python -m bandit -r -c pyproject.toml -q src    # безопасность
venv/bin/python -m pytest tests/unit -q                  # unit
venv/bin/python -m pytest tests/integration -q           # integration (нужен PG)
venv/bin/python -m pytest tests/e2e -q                   # e2e (нужен живой compose)
```

### Pre-commit и CI

```bash
pre-commit install   # ruff, Black, bandit, mypy, unit-тесты и базовые проверки файлов
```

GitHub Actions (`.github/workflows/ci.yml`) — 6 job'ов:
**ruff → mypy → bandit → unit (3.15.2) → integration (PG-сервис) → e2e
(docker compose + chaos-скрипт + DLQ E2E)**. Runtime и test-зависимости закреплены
в `pyproject.toml`, полный hash-lock для Python 3.15 лежит в `requirements.lock`;
Docker и CI устанавливают зависимости из него; версии самих инструментов качества
зафиксированы в workflow, их транзитивные зависимости — нет.

### Время жизни зависимостей и prefetch

Сервисы создаются на каждый запрос (`Depends`-фабрика), UoW хранит сессию и защищён
от повторного входа — иначе параллельные запросы смешали бы транзакции.

Consumer подписан только на `payments.new`. Обработка платежа и HTTP-доставка webhook выполняются последовательно в одном handler, поэтому медленный webhook удерживает сообщение без ACK и занимает слот prefetch. Увеличение prefetch/числа consumer-реплик масштабирует именно этот объединённый путь: выделенного webhook-конвейера и отдельной подписки больше нет.

### Интеграционные тесты

Интеграционные тесты читают DSN **только** из `PAYMENT_TEST_DATABASE_URL`:

```bash
PAYMENT_TEST_DATABASE_URL=postgresql+psycopg://user:password@localhost:5433/payments_test \
  venv/bin/python -m pytest tests/integration -q
```

- `PAYMENT_DATABASE_URL` рабочего стека **не используется как fallback** — иначе тесты
  могли бы писать в рабочую БД.
- Отдельной переменной «пропустить тесты» нет. Если `PAYMENT_TEST_DATABASE_URL` не
  задан, набор скипается целиком; чтобы прогнать его, задайте переменную.
- Нужен DSN именно `postgresql+psycopg://`: изоляция строится на libpq
  `options=-csearch_path=...`.
- Каждый тест работает в собственной временной схеме `pytest_<12 hex>`; `TRUNCATE` и
  `DROP TABLE` не используются, чужие схемы не удаляются. Схема создаётся из
  SQLAlchemy `metadata`, а её паритет с Alembic-миграциями проверяет отдельный unit-тест.

E2E-тесты запускайте против отдельного стенда из быстрого старта в том же Bash: им нужны экспортированные `PAYMENT_API_KEY` и `PAYMENT_WEBHOOK_SECRET`. Остановите ручной приёмник: тесты сами слушают порт 9000. Адрес хоста из контейнера — `host.docker.internal`.

## Структура

```
src/payment/
├── domain/          # агрегат Payment, value objects, события (не знает о фреймворках)
├── application/     # сервисы сценариев (PaymentService, WebhookService, OutboxService) + порты
├── adapters/        # persistence (SQLAlchemy), messaging (FastStream), gateway, webhook
├── entrypoints/     # api (FastAPI), единый consumer, relay
└── bootstrap.py     # composition root

tests/
├── unit/            # домен, сервисы на fake-портах, конфиг, топология, fencing
├── integration/     # репозитории, UoW, outbox, клеймы на реальном PostgreSQL
└── e2e/             # API → outbox → consumer → webhook
```
