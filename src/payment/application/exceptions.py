"""Исключения application-слоя."""


class ApplicationError(Exception):
    """Базовое исключение application-слоя."""


class PaymentBusy(ApplicationError):
    """Платёж в работе у другого consumer'а: сообщение должно уйти в retry, а не ack."""


class PaymentNotFound(ApplicationError):
    def __init__(self, payment_id: str) -> None:
        self.payment_id = payment_id
        super().__init__(f"Payment {payment_id} not found")


class IdempotencyViolation(ApplicationError):
    """Нарушен unique-индекс idempotency_key (гонка двух одновременных POST)."""


class IdempotencyConflict(ApplicationError):
    """Тот же Idempotency-Key, но другое тело запроса."""

    def __init__(self, idempotency_key: str) -> None:
        self.idempotency_key = idempotency_key
        super().__init__(
            f"Idempotency-Key {idempotency_key!r} was already used with a different payload"
        )


class WebhookBusy(ApplicationError):
    """Событие доставки обрабатывается другим consumer'ом: нужен retry, не ack."""

    def __init__(self, event_id: str) -> None:
        self.event_id = event_id
        super().__init__(f"Webhook event {event_id} is being delivered by another consumer")


class WebhookDeliveryFailed(ApplicationError):
    """Отправка webhook не удалась; попытка учтена, сообщение уходит в retry."""

    def __init__(self, event_id: str, attempt: int, error: str) -> None:
        self.event_id = event_id
        self.attempt = attempt
        self.error = error
        super().__init__(f"Webhook event {event_id} attempt {attempt} failed: {error}")


class WebhookDeliveryExhausted(ApplicationError):
    """Все попытки доставки исчерпаны: задача мертва, сообщение идёт в DLQ."""

    def __init__(self, event_id: str, attempts: int, error: str) -> None:
        self.event_id = event_id
        self.attempts = attempts
        self.error = error
        super().__init__(f"Webhook event {event_id} failed after {attempts} attempt(s): {error}")
