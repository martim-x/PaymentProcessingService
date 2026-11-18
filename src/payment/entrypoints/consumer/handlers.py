"""Handler'ы consumer'а: payments.new -> ProcessPayment.

Подписчик живёт в app.py; здесь — чистые вспомогательные функции, которые он использует.
"""

import uuid

from payment.domain.model.value_objects import PaymentId


def is_retry_exhausted(attempt: int, max_attempts: int) -> bool:
    """True, если после этой попытки ретраить нельзя."""
    return attempt >= max_attempts


def parse_payment_id(payload: dict) -> PaymentId:
    return PaymentId(value=uuid.UUID(str(payload["payment_id"])))
