# Архитектура Payment Processing Service

Документ описывает процессы сервиса, границы слоёв и потоки данных.

- Запуск, HTTP-примеры и ограничения эксплуатации: [`README.md`](../README.md).
- Визуальная схема не входит в репозиторий; актуальная текстовая схема потоков приведена ниже.

## Процессы и конвейеры

Сервис состоит из шести контейнеров: `api`, `relay` и единственный `consumer` работают постоянно, `migrate` — одноразовый, плюс инфраструктурные PostgreSQL и RabbitMQ. Отдельного webhook-worker нет.

| Процесс | Роль | Точка входа |
|---|---|---|
| **api** | HTTP-интерфейс: приём платежа, чтение статуса | `payment.entrypoints.api.app:app` |
| **relay** | Поллинг outbox и публикация событий в RabbitMQ | `payment.entrypoints.relay.runner` |
| **consumer** | Одна активная подписка `payments.new`; единый handler обрабатывает платёж, затем webhook до ACK | `payment.entrypoints.consumer.app` |
| **migrate** | `alembic upgrade head` перед стартом остальных | сервис `migrate` в `docker-compose.yml` |

Путь обработки проходит через outbox и единственный consumer:

```text
POST /api/v1/payments
  api ── INSERT payments + outbox(payment.created) ── одной транзакцией ──▶ PostgreSQL
                                                                              ▲
relay ── SELECT ... WHERE sent=false AND failed=false AND dispatch='relay' FOR UPDATE SKIP LOCKED ─┘
  └─ payment.created → payments.new

`payments.new → единственный handler → PaymentService.process_payment → claim → шлюз → финальный статус + outbox(payment.webhook)`
`outbox(payment.webhook) [dispatch=local: relay её не видит; sent не выставляется]`
`                 → WebhookService.deliver → HTTP → webhook_deliveries.status`
`                 → ack | retry через payments.new TTL-цепочку | payments.dlq`
```

Единственная подписка consumer — `payments.new`; webhook выполняется тем же handler. Retry-лестницы существуют отдельно для обычных и инфраструктурных ошибок, но отдельной очереди/подписки webhook нет. Повторы используют общий лимит; после исчерпания сообщение направляется в `payments.dlq`.

## Прикладные сценарии

Создание платежа:

`entrypoints/api/routers/payments.py` → `application/services/payment_service.py` (`create_payment`) → `domain/model/payment.py` и порты → реализации в `adapters/persistence/`.

Обработка платежа:

`entrypoints/consumer/app.py` → `application/services/payment_service.py` (`process_payment`) → доменный `Payment` и порты → адаптеры шлюза и БД. Затем consumer вызывает `WebhookService.deliver` до ACK.

Публикация события:

`entrypoints/relay/runner.py` → `application/services/outbox_service.py` (`publish_pending`) → порт publisher/repository → адаптеры PostgreSQL и RabbitMQ.

Доставка webhook вызывается внутри обработчика: `entrypoints/consumer/app.py` → `application/services/webhook_service.py` (`deliver`) → порты `WebhookSender` и `WebhookDeliveryRepository`.

## Главное правило зависимостей

```text
Внешний мир → entrypoints → application → domain
                         adapters ────────┘
                         bootstrap связывает реализации с портами
```

- **Domain** содержит понятия и правила предметной области. Он не должен зависеть от FastAPI, SQLAlchemy или FastStream.
- **Application** описывает сценарии и нужные им интерфейсы (порты). Сценарий зависит от контракта, например `PaymentGateway`, а не от конкретного шлюза.
- **Adapters** реализуют порты через технологии: БД, брокер, HTTP.
- **Entrypoints** принимают внешние запросы/сообщения и вызывают сценарии.
- **`bootstrap.py`** собирает конкретные реализации вместе. Это composition root, а не бизнес-слой.

Направление вызова во время работы и направление зависимости — не одно и то же. Например, сервис вызывает порт publisher, а конкретный `FastStreamPublisher` находится снаружи и реализует этот порт.

## Каталоги и файлы

### `src/payment/domain/` — предметная модель

- `model/payment.py` — `Payment`, агрегатный корень (Aggregate Root) в модели проекта. Создаёт платёж, управляет допустимыми переходами статуса и собирает доменные события. Это основной объект, через который меняется жизненный цикл платежа.
- `model/value_objects.py` — `Money`, `PaymentId`, `IdempotencyKey`, `WebhookUrl`: значения с проверкой инвариантов. Они описывают смысл данных, а не способ их хранения.
- `model/enums.py` — допустимые валюты и статусы.
- `model/fingerprint.py` — канонизация запроса и fingerprint для сравнения повторов ключа идемпотентности.
- `events.py` — факты домена (`PaymentCreated`, `PaymentSucceeded`, `PaymentFailed`). Они описывают, что произошло; сообщение RabbitMQ — транспортная форма доставки, не само доменное событие.
- `policies/retry_policy.py` — политика расчёта числа/задержки повторов. Это stateless Policy: получает входные значения и рассчитывает результат.
- `exceptions.py` — ошибки нарушения доменных правил.

### `src/payment/application/` — сценарии и их контракты

Сценарии собраны в три сервиса: `PaymentService`, `WebhookService`, `OutboxService`. Сервис оркестрирует шаги, но не содержит правил допустимых состояний — их проверяет агрегат `Payment`.

- `services/payment_service.py` — `PaymentService`:
  - `create_payment(command)`: проверяет идемпотентность, создаёт `Payment`, добавляет запись outbox через UoW и фиксирует их одной транзакцией;
  - `get_payment(payment_id)`: читает платёж и готовит DTO ответа;
  - `process_payment(payment_id)`: берёт claim платежа отдельной короткой транзакцией, вызывает шлюз уже без транзакции, затем фиксирует статус (`try_finalize`) и запись outbox `payment.webhook` одной транзакцией. Webhook здесь не отправляется.
- `services/webhook_service.py` — `WebhookService.deliver(command)`: идемпотентно создаёт задачу доставки, берёт клейм, отправляет HTTP-запрос, записывает итог под fencing token. Повторная доставка после падения между отправкой и записью статуса возможна — это at-least-once.
- `services/outbox_service.py` — `OutboxService.publish_pending(batch=...)`: читает неотправленные записи, маршрутизирует по `event_type`, публикует и отмечает результат.
- `ports/` — контракты, необходимые сценариям:
  - `repositories.py`: `PaymentRepository`, `OutboxRepository`, `WebhookDeliveryRepository`, `OutboxRecord`, `WebhookDelivery`;
  - `ports/unit_of_work.py`: `AbstractUnitOfWork` предоставляет сценариям репозитории, контекст транзакции и явный `commit`; при исключении незавершённую транзакцию откатывает адаптер.
  - `payment_gateway.py`: `PaymentGateway`, `GatewayResult`;
  - `webhook_sender.py`: `WebhookSender`;
  - `message_publisher.py`: `MessagePublisher`, `PublisherUnavailable`, `PublishRejected`.
- `dto.py` — объекты передачи данных между входной точкой и сервисом; это не доменные сущности и не API-схемы.
- `exceptions.py` — ошибки уровня сценариев: платёж не найден, конфликт идемпотентности, `PaymentBusy`, `WebhookBusy`, `WebhookDeliveryFailed`, `WebhookDeliveryExhausted`.

### `src/payment/adapters/` — конкретные реализации контрактов

- `persistence/orm.py` — SQLAlchemy-описание таблиц `payments`, `outbox`, `webhook_deliveries`.
- `persistence/database.py` — создание async engine и sessionmaker.
- `persistence/unit_of_work.py` — `SqlAlchemyUnitOfWork`: управляет сессией и транзакцией; коммит атомарен для всех трёх таблиц.
- `persistence/repositories/payment.py` — преобразует строки БД в `Payment` и обратно; выполняет запросы платежей и claim/finalize.
- `persistence/repositories/outbox.py` — запросы и обновление записей outbox, `fetch_unsent` с `FOR UPDATE SKIP LOCKED`.
- `persistence/repositories/webhook.py` — задача доставки: `create_if_absent`, `try_claim`, `release_claim`, `mark_delivered`, `mark_attempt_failed`; все переходы условны по токену клейма.
- `messaging/publisher.py` — `FastStreamPublisher`, реализация порта `MessagePublisher`; различает недоступность транспорта и отказ конкретной записи.
- `messaging/topology.py` — объявления основной очереди, двух лестниц retry (`retry.N` и `infra.retry.N`) и DLQ; бюджет отказов у лестниц общий.
- `messaging/routing.py` — выбор маршрута при неудачной обработке и номер попытки.
- `messaging/redelivery.py` — публикация копии в retry/DLQ и только затем `ack`; при сбое публикации — `nack requeue`.
- `messaging/schemas.py` — структура транспортного сообщения (`PaymentCreatedMessage`); отдельного конверта webhook-очереди нет.
- `messaging/connection.py` — подключение к брокеру с повторами.
- `gateway/fake_gateway.py` — тестовая эмуляция внешнего платёжного шлюза, реализация `PaymentGateway`.
- `webhook/http_sender.py` — HTTP-отправка webhook, реализация `WebhookSender`.
- `webhook/signer.py` — сериализация и HMAC-подпись тела webhook.
- `webhook/guard.py` — проверка адреса webhook и защита от SSRF.

### `src/payment/entrypoints/` — входы в приложение

- `api/app.py` — создание FastAPI-приложения, lifespan, `/health`.
- `api/routers/payments.py` — HTTP-маршруты создания/получения платежа; преобразуют запрос в команду и вызывают `PaymentService`.
- `api/schemas.py` — Pydantic-модели HTTP-запроса/ответа и лимиты полей. Это формат API, не доменная модель.
- `api/dependencies.py` — проверка `X-API-Key`, `Idempotency-Key` и создание `PaymentService` для запроса.
- `api/error_handlers.py` — отображение ошибок приложения/домена в HTTP-ответы.
- `consumer/app.py` — регистрирует одну подписку на `payments.new`. Её handler вызывает `PaymentService.process_payment`, затем `WebhookService.deliver`, и подтверждает сообщение после успеха. Ошибки маршрутизируют исходное сообщение через retry/DLQ основного конвейера. Отдельной очереди и подписчика для webhook нет.

- `relay/runner.py` — периодически запускает публикацию outbox и обрабатывает завершение процесса.

### Сборка и конфигурация

- `bootstrap.py` — `Container`: создаёт адаптеры, брокер с каналом (prefetch, publisher confirms, `on_return_raises`) и фабрики сервисов (свой UoW на каждый экземпляр). Здесь одна политика бюджета попыток и две базы задержки: обычные ретраи и более медленные инфраструктурные.
- `config.py` — настройки окружения и проверка их совместимости (см. ниже).
- `migrations/versions/20261118_0001_initial.py` — `payments` и `outbox`.
- `migrations/versions/20261119_0002_webhook_deliveries.py` — `webhook_deliveries` и индекс `ix_outbox_event_type`.
- `migrations/versions/20261120_0003_outbox_dispatch.py` — колонка `outbox.dispatch` и частичный индекс `ix_outbox_relay_pending`.
- `docker-compose.yml` — postgres, rabbitmq (с volume), migrate, api, relay и один consumer.

## Состояние: что и где хранится

| Таблица | Ключевые поля | Смысл |
|---|---|---|
| `payments` | `id`, `amount Numeric(19,4)`, `currency`, `status`, `idempotency_key UNIQUE`, `fingerprint`, `webhook_url`, `locked_until` | Платёж и его жизненный цикл `pending → succeeded/failed`; `locked_until` — клейм обработки |
| `outbox` | `id`, `aggregate_id`, `event_type`, `payload JSONB`, `sent`, `dispatch`, `failed`, `attempts` | Журнал событий: `payment.created` (`dispatch=relay`) публикуется relay; `payment.webhook` (`dispatch=local`) хранится для возобновления доставки и никогда не публикуется |
| `webhook_deliveries` | `event_id PK`, `payment_id`, `status`, `attempts`, `last_error`, `locked_until`, `delivered_at` | Фактический статус и попытки HTTP-доставки; ключ — стабильный `event_id` из outbox |

`event_type` в outbox — не косметика: relay выбирает по нему очередь. Если типа нет ни в `event_type`, ни в `payload["event"]`, запись уходит в очередь по умолчанию (`payments.new`) и, скорее всего, будет отбракована consumer'ом как poison.

## Инварианты и где они проверяются

| Инвариант | Где закреплён |
|---|---|
| Финальный статус платежа нельзя изменить | `Payment.mark_succeeded/mark_failed` → `InvalidStatusTransition` |
| Платёж и его событие появляются вместе | `PaymentService._create_payment` и `PaymentService._finalize` в одном UoW |
| Один платёж не обрабатывают параллельно | `try_claim` (условный `UPDATE` по `status='pending'` и свободному `locked_until`) |
| Итог обработки может записать только владелец клейма | `try_finalize` фильтрует по `locked_until = claim_token` |
| Один webhook не отправляют параллельно | `webhook_deliveries.try_claim` + `mark_delivered/mark_attempt_failed` по токену |
| Дубликат сообщения не создаёт вторую задачу доставки | `create_if_absent` (`ON CONFLICT DO NOTHING` по `event_id`) |
| Повтор API с тем же ключом не создаёт платёж | unique `idempotency_key` + fingerprint; гонка → перечитывание |
| Повтор с другим телом не проходит | сравнение fingerprint → `IdempotencyConflict` → `409` |
| Сообщение не теряется при сбое публикации retry/DLQ | `redelivery.publish_or_requeue` (сначала publish, потом ack) |
| Падение брокера не «сжигает» outbox | `PublisherUnavailable` не увеличивает `attempts` |
| Тайминги клеймов и ретраев согласованы | `Settings`-валидаторы в `config.py` |
| Схема миграций совпадает с `metadata` | `tests/unit/adapters/persistence/test_migration_schema_parity.py` |

Числовые значения (TTL retry-очередей 1 и 2 с, infra-лестница 2 и 4 с, клейм 30 с, бюджет ожидания дубликата 48 с) и их обоснование — в [`README.md`](../README.md#технические-решения).

## Текущий runtime и границы документации

1. **Один активный конвейер consumer.** `docker-compose.yml` запускает consumer, который подписывается только на `payments.new`. Один handler выполняет `PaymentService.process_payment`, затем `WebhookService.deliver` до ACK. Отдельной webhook-очереди или webhook-подписчика нет.
2. **Сохранённый payload webhook не означает его отправку.** Во время финализации команда `payment.webhook` пишется в outbox с `dispatch='local'`: relay её не читает, `sent` не выставляется. Результат реальной HTTP-доставки ведётся в `webhook_deliveries`.
3. **Повторная обработка.** Финализированный платёж не должен повторно проходить шлюз; при сбое webhook исходное сообщение `payments.new` повторяется, а `webhook_deliveries` обеспечивает сохранение состояния и идемпотентность доставки.
4. **Стабильный `event_id`.** При финализации генерируется `uuid4().hex`; идентификатор хранится в `outbox` вместе с payload webhook и не меняется при повторной доставке. Исходное сообщение `payment.created` этого идентификатора не содержит.
5. **Семантика повторов — at-least-once, не exactly-once.** Дубли возможны в очереди (перепубликация) и в доставке (падение между HTTP-ответом и записью `delivered`). Успешная доставка webhook не гарантирована: после исчерпания попыток сообщение уходит в DLQ. Защита от повторных эффектов — ключи идемпотентности: `Idempotency-Key`, id платежа как ключ шлюза, `event_id` у получателя.
6. **Relay публикует внутри транзакции.** `FOR UPDATE SKIP LOCKED` берётся при выборке, `commit` — после публикации батча. Это упрощает код (нет статуса «publishing» и reclaim-логики), но блокировки строк живут, пока идёт публикация.

Полный список эксплуатационных ограничений — в [`README.md`](../README.md#ограничения-осознанные).

## Безопасность: где в коде и чего нет

| Контроль | Где | Граница |
|---|---|---|
| `X-API-Key` | `api/dependencies.py`, `hmac.compare_digest` | Один статический ключ, без ротации и скоупов |
| Секреты обязательны | `config.py` (`_INSECURE_DEFAULTS`) | Только env/compose, без vault |
| SSRF-гард | `webhook/guard.py`, `WebhookUrl` | Приватные адреса запрещены по умолчанию; для E2E разрешаются явно. DNS-проверка не исключает rebinding |
| HMAC-подпись | `webhook/signer.py` | Один общий секрет на всех получателей |
| Валидация входа | `api/schemas.py`, `api/dependencies.py` | Нет лимита на тело целиком и rate limiting |
| Непривилегированный контейнер | `Dockerfile` (uid 10001) | Инфраструктура (RabbitMQ UI, PostgreSQL) опубликована на хост |

Чего в сервисе нет: TLS, ротации ключей, per-endpoint секретов, шифрования at-rest, аудита доступа, антифрода и аутентификации на DLQ/management UI. Это осознанные границы учебного проекта; перечень — в [`README.md`](../README.md#ограничения-безопасности).

## Назначение основных абстракций

| Абстракция | Назначение | Реализация |
|---|---|---|
| Entity | Объект с идентичностью и меняющимся состоянием | `Payment` |
| Value Object | Значение с правилами, без отдельной идентичности | `Money`, `PaymentId`, `WebhookUrl` |
| Aggregate Root | Единственная точка изменения агрегата; защищает его инварианты | `Payment` |
| Domain Event | Факт, уже произошедший в домене | `PaymentCreated`, `PaymentSucceeded` |
| Policy | Отдельное stateless-правило: входные данные → решение/расчёт | `ExponentialBackoffRetryPolicy` |
| Application service | Один прикладной сценарий (набор методов), координирующий шаги | `PaymentService`, `WebhookService`, `OutboxService` |
| Port | Контракт того, что сценарию требуется от внешнего мира | `PaymentGateway`, `WebhookDeliveryRepository` |
| Adapter | Технологическая реализация порта | `FakePaymentGateway`, `SqlAlchemyWebhookDeliveryRepository` |
| DTO | Данные, передаваемые между границами сценария | `CreatePaymentCommand`, `PaymentDetails` |
| Unit of Work | Граница транзакции и совместной фиксации изменений | `AbstractUnitOfWork`, `SqlAlchemyUnitOfWork` |
| Composition root | Место, где выбираются и соединяются реализации | `Container` в `bootstrap.py` |

## Доменная модель

`Payment` — агрегатный корень, который управляет переходами между состояниями платежа.

`ExponentialBackoffRetryPolicy` вычисляет задержку повторной попытки. `PaymentCreated` — доменное событие; `PaymentCreatedMessage` — формат сообщения очереди. `payment.webhook` — сохранённая команда доставки уведомления.

Сервис ограничен контекстом обработки платежей; модель заказов в него не входит.

## Покрытие тестами

- `tests/unit/` — домен, сервисы на fake-портах, конфигурационные инварианты, топология и маршрутизация, fencing-предикаты SQL, паритет схемы миграций.
- `tests/integration/` — репозитории, UoW, outbox и клеймы на реальном PostgreSQL. DSN берётся только из `PAYMENT_TEST_DATABASE_URL`; без неё набор скипается, fallback на рабочую БД запрещён.
- `tests/e2e/` — полный поток против compose, включая проверку HMAC от сырых байтов.
