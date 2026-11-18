"""Конфигурация сервиса (pydantic-settings, префикс PAYMENT_).

Секреты обязательны: приложение не стартует с дефолтными значениями.
"""

from functools import lru_cache

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

_INSECURE_DEFAULTS = {
    "",
    "change-me",
    "change-me-secret-api-key",
    "change-me-webhook-signing-secret",
}


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="PAYMENT_", env_file=".env", extra="ignore")

    # API
    api_key: str = Field(default="", description="X-API-Key; обязателен")

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
    webhook_secret: str = Field(default="", description="HMAC-секрет; обязателен")
    # true только для локальных стендов/тестов: host.docker.internal — частный адрес
    webhook_allow_private_hosts: bool = False

    # Retry (RabbitMQ TTL-очереди и webhook-ретраи)
    max_attempts: int = 3
    retry_base_delay: float = 1.0
    retry_factor: float = 2.0

    @model_validator(mode="after")
    def _check_secrets(self) -> "Settings":
        if self.api_key.strip().lower() in _INSECURE_DEFAULTS:
            raise ValueError(
                "PAYMENT_API_KEY is not set (or left at the insecure default); "
                "set a real API key before starting the service"
            )
        if self.webhook_secret.strip().lower() in _INSECURE_DEFAULTS:
            raise ValueError(
                "PAYMENT_WEBHOOK_SECRET is not set (or left at the insecure default); "
                "set a real webhook signing secret"
            )
        return self


@lru_cache
def get_settings() -> Settings:
    return Settings()
