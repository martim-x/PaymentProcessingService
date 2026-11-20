"""Порты: протоколы UoW и репозиториев."""

from dataclasses import dataclass
from datetime import datetime
from types import TracebackType
from typing import Protocol
from uuid import UUID

from payment.domain.model.payment import Payment
from payment.domain.model.value_objects import PaymentId

# Статусы задачи доставки webhook (таблица webhook_deliveries).
DELIVERY_PENDING = "pending"
DELIVERY_DELIVERING = "delivering"
DELIVERY_DELIVERED = "delivered"
DELIVERY_FAILED = "failed"

# Кто публикует запись outbox: relay ('relay') или никто — строка хранится для
# consumer'а ('local'). Второй режим нужен, чтобы не выдавать запись за
# отправленную: sent остаётся честным флагом публикации.
DISPATCH_RELAY = "relay"
DISPATCH_LOCAL = "local"


@dataclass(slots=True)
class OutboxRecord:
    """Строка outbox (доменно-нейтральная).

    `event_type` — тип события (`payment.created`, `payment.webhook`):
    relay маршрутизирует запись в очередь по нему, а не по телу payload.
    `dispatch` — `relay` (публикуется) или `local` (хранится для consumer'а).
    """

    id: object
    aggregate_id: str
    payload: dict[str, object]
    sent: bool = False
    attempts: int = 0
    failed: bool = False
    event_type: str = ""
    dispatch: str = DISPATCH_RELAY


@dataclass(slots=True)
class WebhookDelivery:
    """Состояние доставки одного webhook-события.

    Ключ — стабильный `event_id` из outbox-записи: повторная публикация того же
    события (relay, ретрай брокера) не создаёт вторую задачу доставки.
    `locked_until` — fencing token текущего владельца: только он может записать
    итог доставки.
    """

    event_id: str
    payment_id: UUID
    status: str = DELIVERY_PENDING
    attempts: int = 0
    last_error: str | None = None
    locked_until: datetime | None = None


class PaymentRepository(Protocol):
    async def add(self, payment: Payment) -> None:
        """Пишет платёж в текущую транзакцию; коммит делает UoW.

        Конфликт unique(idempotency_key) отдаётся как IdempotencyViolation,
        чтобы сервис перечитал платёж-победитель, а не получил 500.
        """
        ...

    async def try_claim(self, payment_id: PaymentId, ttl_seconds: float) -> datetime | None:
        """Атомарный клейм обработки: pending -> locked_until = now() + ttl.

        Возвращает токен клейма (UTC-время), либо None, если платёж уже
        финализирован или заклеймлен другим consumer'ом. Клейм истекает,
        поэтому упавший consumer не «замораживает» платёж навсегда.
        """
        ...

    async def release_claim(self, payment_id: PaymentId, claim_token: datetime) -> None:
        """Снимает клейм, если он всё ещё принадлежит claim_token."""
        ...

    async def try_finalize(self, payment: Payment, claim_token: datetime) -> bool:
        """Атомарно фиксирует финальный статус под клеймом claim_token.

        Возвращает False, если платёж уже обработан/клейм устарел —
        в этом случае побочные эффекты (webhook) повторять нельзя.
        """
        ...

    async def get(self, payment_id: PaymentId) -> Payment | None: ...

    async def get_by_idempotency_key(self, key: str) -> Payment | None: ...


class OutboxRepository(Protocol):
    async def find_event(self, *, aggregate_id: str, event_type: str) -> OutboxRecord | None: ...

    async def add(
        self,
        *,
        payload: dict[str, object],
        aggregate_id: str,
        dispatch: str = DISPATCH_RELAY,
    ) -> OutboxRecord:
        """Пишет событие в текущую транзакцию — ту же, что и сам агрегат.

        `event_type` выводится из `payload["event"]`: по нему relay выбирает очередь.
        `dispatch='local'` помечает строку как состояние для consumer'а: relay её
        не публикует, `sent` остаётся false.
        """
        ...

    async def fetch_unsent(self, batch: int = 100) -> list[OutboxRecord]:
        """Неотправленные и публикуемые записи пачкой, старые первыми.

        Записи захватываются на время транзакции (в SQL-адаптере — SKIP LOCKED),
        поэтому параллельный relay не получит ту же запись. Гарантия — at-least-once:
        падение после публикации, но до commit оставляет запись неотправленной,
        и событие уйдёт повторно. Получатель дедуплицирует по message_id/event_id.
        Строки с `dispatch='local'` не выбираются никогда.
        """
        ...

    async def mark_sent(self, record_id: object) -> None: ...

    async def mark_attempt_failed(self, record_id: object, max_attempts: int) -> bool:
        """Инкремент attempts; True — запись окончательно помечена failed."""
        ...


class WebhookDeliveryRepository(Protocol):
    """Задача доставки webhook: идемпотентное создание + клейм от дубликатов."""

    async def create_if_absent(self, *, event_id: str, payment_id: UUID) -> WebhookDelivery:
        """INSERT ... ON CONFLICT DO NOTHING: повтор сообщения не плодит задачи."""
        ...

    async def try_claim(self, event_id: str, ttl_seconds: float) -> datetime | None:
        """Клейм доставки: pending/delivering с истёкшим клеймом -> delivering.

        Возвращает fencing token (UTC-время истечения) либо None: событие уже
        доставлено/мертво или его прямо сейчас обрабатывает другой consumer
        (нельзя отправлять второй webhook параллельно).
        """
        ...

    async def release_claim(self, event_id: str, claim_token: datetime) -> None:
        """Снимает клейм, только если он всё ещё принадлежит claim_token."""
        ...

    async def mark_delivered(self, event_id: str, claim_token: datetime) -> bool:
        """Фиксирует доставку ТОЛЬКО под своим клеймом (fencing token).

        False — клейм устарел (истёк TTL, событие перехватил новый владелец):
        состояние не изменено, перетирать чужой прогресс нельзя.
        """
        ...

    async def mark_attempt_failed(
        self, event_id: str, claim_token: datetime, max_attempts: int, error: str
    ) -> tuple[int, bool] | None:
        """+1 к attempts под своим клеймом; (всего попыток, окончательно ли упало).

        None — клейм устарел (fencing): попытка НЕ учтена, состояние не тронуто.
        """
        ...

    async def get(self, event_id: str) -> WebhookDelivery | None: ...


class AbstractUnitOfWork(Protocol):
    """Единица работы: коммит атомарен для payments + outbox + webhook_deliveries."""

    payments: PaymentRepository
    outbox: OutboxRepository
    webhook_deliveries: WebhookDeliveryRepository

    async def __aenter__(self) -> "AbstractUnitOfWork": ...

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None: ...

    async def commit(self) -> None: ...

    async def rollback(self) -> None: ...
