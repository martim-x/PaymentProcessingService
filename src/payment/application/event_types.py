"""Stable identifiers for application-level outbox events."""

from enum import StrEnum


class OutboxEventType(StrEnum):
    PAYMENT_CREATED = "payment.created"
    PAYMENT_WEBHOOK = "payment.webhook"
