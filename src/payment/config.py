"""Конфигурация сервиса (pydantic-settings, префикс PAYMENT_)."""

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="PAYMENT_", env_file=".env", extra="ignore")

    # API
    api_key: str = "change-me-secret-api-key"

    # PostgreSQL
    database_url: str = "postgresql+psycopg://payment:payment@postgres:5432/payments"

    # RabbitMQ
    rabbitmq_url: str = "amqp://guest:guest@rabbitmq:5672/"
    rabbitmq_queue: str = "payments.new"

    # Outbox relay
    relay_interval: float = 1.0
    relay_batch: int = 100

    # Обработка платежей (эмуляция шлюза)
    process_min_seconds: float = 2.0
    process_max_seconds: float = 5.0
    success_rate: float = 0.9

    # Webhook
    webhook_timeout: float = 5.0
    webhook_secret: str = "change-me-webhook-signing-secret"

    # Retry
    max_attempts: int = 3
    retry_base_delay: float = 1.0
    retry_factor: float = 2.0


@lru_cache
def get_settings() -> Settings:
    return Settings()
