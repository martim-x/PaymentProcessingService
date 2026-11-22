"""Конфигурация сервиса (pydantic-settings, префикс PAYMENT_).

Секреты обязательны: приложение не стартует с дефолтными значениями.
"""

from functools import lru_cache
from typing import ClassVar

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from payment.domain.policies.retry_policy import ExponentialBackoffRetryPolicy


class Settings(BaseSettings):
    """Настройки сервиса из переменных окружения с префиксом `PAYMENT_`."""

    _INSECURE_DEFAULTS: ClassVar[frozenset[str]] = frozenset(
        {
            "",
            "change-me",
            "change-me-secret-api-key",
            "change-me-webhook-signing-secret",
            "change-this-api-key",
            "change-this-webhook-secret",
        }
    )

    model_config = SettingsConfigDict(
        env_prefix="PAYMENT_",
        env_file=".env",
        extra="ignore",
    )

    # API
    api_key: str = Field(
        default="",
        description="X-API-Key; обязателен",
    )

    # PostgreSQL
    database_url: str = "postgresql+psycopg://payment:payment@postgres:5432/payments"

    # RabbitMQ: consumer последовательно обрабатывает платёж и доставляет webhook
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
    webhook_secret: str = Field(
        default="",
        description="HMAC-секрет; обязателен",
    )
    # true только для локальных стендов/тестов: host.docker.internal — частный адрес
    webhook_allow_private_hosts: bool = False

    # Retry (RabbitMQ TTL-очереди и webhook-ретраи)
    max_attempts: int = 3
    retry_base_delay: float = 1.0
    retry_factor: float = 2.0
    # Транзиентные сбои БД/транспорта: отдельная лестница задержек (счётчик
    # x-infra-attempt задаёт её ступень), но общий с max_attempts бюджет отказов.
    infra_retry_base_delay: float = 2.0

    # Клейм обработки платежа: страхует от зависшего consumer'а.
    # TTL покрывает шлюз (process_max) + финализацию с запасом.
    claim_ttl_seconds: float = 30.0
    # сколько ждать освобождения клейма дубликату сообщения (до retry/DLQ)
    claim_wait_seconds: float = 15.0
    # период опроса клейма ожидающим дубликатом
    claim_poll_seconds: float = 0.5
    # запас на финализацию: TTL клейма обязан быть больше process_max на эту величину
    claim_finalize_margin_seconds: float = 5.0
    # Клейм доставки webhook: пока он жив, дубликат события не отправит второй запрос.
    # TTL покрывает HTTP-таймаут webhook с запасом.
    webhook_claim_ttl_seconds: float = 30.0
    # Сколько раз relay пытается опубликовать «ядовитую» запись outbox
    outbox_max_attempts: int = 10
    # Prefetch consumer'ов: сколько сообщений канал берёт из очереди одновременно
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
            self.retry_base_delay * (self.retry_factor**i)
            for i in range(self.max_attempts - 1)
        )
        return waits + delays

    @property
    def infra_retry_policy(self) -> ExponentialBackoffRetryPolicy:
        """Политика ретраев транзиентных инфраструктурных сбоев.

        Отличается от политики конвейера только базой задержки: граница попыток
        общая, иначе суммарное число отказов перестало бы равняться max_attempts.
        """
        return ExponentialBackoffRetryPolicy(
            max_attempts=self.max_attempts,
            base_delay=self.infra_retry_base_delay,
            factor=self.retry_factor,
        )

    @model_validator(mode="after")
    def _check_numeric_limits(self) -> "Settings":
        """Запрещает значения, превращающие retry-цепочки в горячий цикл.

        Определён до проверок бюджета: иначе отрицательная база задержки
        «съест» бюджет и отчёт будет про claim TTL, а не про виновника.
        """
        if self.max_attempts < 2:
            raise ValueError(
                "PAYMENT_MAX_ATTEMPTS "
                f"({self.max_attempts}) must be at least 2: with a single attempt a "
                "transient failure goes straight to the DLQ and the payment stays pending"
            )
        if self.retry_base_delay <= 0:
            raise ValueError(
                "PAYMENT_RETRY_BASE_DELAY "
                f"({self.retry_base_delay}) must be > 0: a zero-TTL retry queue "
                "redelivers the message immediately, which is a hot requeue loop"
            )
        if self.retry_factor < 1:
            raise ValueError(
                "PAYMENT_RETRY_FACTOR "
                f"({self.retry_factor}) must be >= 1.0: a shrinking delay turns "
                "exponential backoff into a busy retry loop"
            )
        if self.infra_retry_base_delay <= 0:
            raise ValueError(
                "PAYMENT_INFRA_RETRY_BASE_DELAY "
                f"({self.infra_retry_base_delay}) must be > 0: a zero-TTL infra retry "
                "queue redelivers the message immediately"
            )
        if self.consumer_prefetch < 1:
            raise ValueError(
                "PAYMENT_CONSUMER_PREFETCH "
                f"({self.consumer_prefetch}) must be >= 1: 0 means unlimited prefetch "
                "in aio-pika, so a backlog would starve the webhook consumer"
            )
        if self.claim_poll_seconds <= 0:
            raise ValueError(
                "PAYMENT_CLAIM_POLL_SECONDS "
                f"({self.claim_poll_seconds}) must be > 0: polling the claim in a "
                "tight loop burns CPU instead of waiting for the lease to expire"
            )
        if self.claim_wait_seconds <= 0:
            raise ValueError(
                "PAYMENT_CLAIM_WAIT_SECONDS "
                f"({self.claim_wait_seconds}) must be > 0: a duplicate must give the "
                "running consumer a chance to finish before it spends a retry attempt"
            )
        return self

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
    def _check_webhook_claim_ttl(self) -> "Settings":
        minimum = self.webhook_timeout + self.claim_finalize_margin_seconds
        if self.webhook_claim_ttl_seconds < minimum:
            raise ValueError(
                "PAYMENT_WEBHOOK_CLAIM_TTL_SECONDS "
                f"({self.webhook_claim_ttl_seconds}) must be at least "
                "PAYMENT_WEBHOOK_TIMEOUT + PAYMENT_CLAIM_FINALIZE_MARGIN_SECONDS "
                f"({minimum}s): a shorter claim lets a duplicate delivery start while "
                "the first HTTP request is still in flight."
            )
        return self

    @model_validator(mode="after")
    def _check_secrets(self) -> "Settings":
        if self.api_key.strip().lower() in self._INSECURE_DEFAULTS:
            raise ValueError(
                "PAYMENT_API_KEY is not set (or left at the insecure default); "
                "set a real API key before starting the service"
            )
        if self.webhook_secret.strip().lower() in self._INSECURE_DEFAULTS:
            raise ValueError(
                "PAYMENT_WEBHOOK_SECRET is not set (or left at the insecure default); "
                "set a real webhook signing secret"
            )
        return self

    @classmethod
    @lru_cache
    def from_environment(cls) -> "Settings":
        """Загружает и кеширует конфигурацию процесса."""
        return cls()
