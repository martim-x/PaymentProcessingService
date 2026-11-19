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
    rabbitmq_dlq: str = "payments.dlq"

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

    # Клейм обработки платежа: страхует от зависшего consumer'а.
    # TTL покрывает шлюз (process_max) + финализацию с запасом.
    claim_ttl_seconds: float = 30.0
    # сколько ждать освобождения клейма дубликату сообщения (до retry/DLQ)
    claim_wait_seconds: float = 15.0
    # период опроса клейма ожидающим дубликатом
    claim_poll_seconds: float = 0.5
    # запас на финализацию: TTL клейма обязан быть больше process_max на эту величину
    claim_finalize_margin_seconds: float = 5.0
    # Сколько раз relay пытается опубликовать «ядовитую» запись outbox
    outbox_max_attempts: int = 10
    # Prefetch consumer'а: сколько сообщений он берёт из очереди одновременно
    consumer_prefetch: int = 10

    @property
    def retry_wait_budget(self) -> float:
        """Сколько секунд дубликат в сумме может ждать истечения клейма.

        max_attempts ожиданий по claim_wait_seconds + TTL retry-очередей
        между попытками. Бюджет обязан покрывать claim_ttl_seconds, иначе при
        жёстком падении consumer'а все попытки истекут раньше, чем клейм
        освободится, и сообщение уйдёт в DLQ с платежом в pending.
        """
        waits = self.max_attempts * self.claim_wait_seconds
        delays = sum(
            self.retry_base_delay * (self.retry_factor**i) for i in range(self.max_attempts - 1)
        )
        return waits + delays

    @model_validator(mode="after")
    def _check_claim_lower_bound(self) -> "Settings":
        minimum = self.process_max_seconds + self.claim_finalize_margin_seconds
        if self.claim_ttl_seconds < minimum:
            raise ValueError(
                "PAYMENT_CLAIM_TTL_SECONDS "
                f"({self.claim_ttl_seconds}) must be at least "
                f"PAYMENT_PROCESS_MAX_SECONDS + margin ({minimum}s): a shorter claim "
                "lets a second consumer steal the payment from a consumer that is "
                "still talking to the gateway; only the gateway idempotency key "
                "would prevent a double charge."
            )
        return self

    @model_validator(mode="after")
    def _check_claim_budget(self) -> "Settings":
        safety = 2 * self.claim_poll_seconds
        # строгое неравенство: бюджет обязан превышать TTL хотя бы на два опроса
        if not self.claim_ttl_seconds + safety < self.retry_wait_budget:
            raise ValueError(
                "PAYMENT_CLAIM_TTL_SECONDS "
                f"({self.claim_ttl_seconds}) exceeds the duplicate-wait budget "
                f"({self.retry_wait_budget:.1f}s = {self.max_attempts} x "
                f"PAYMENT_CLAIM_WAIT_SECONDS + retry TTLs): a duplicate would exhaust "
                "all retries before the stale claim expires and the payment would "
                "stay pending. The budget must exceed the TTL by at least two poll "
                "intervals: increase PAYMENT_CLAIM_WAIT_SECONDS or lower "
                "PAYMENT_CLAIM_TTL_SECONDS."
            )
        return self

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
