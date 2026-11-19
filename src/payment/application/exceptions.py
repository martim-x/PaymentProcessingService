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
