"""Composition root: единственное место сборки зависимостей."""

from faststream.rabbit import RabbitBroker
from faststream.rabbit.schemas import Channel
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from payment.adapters.gateway.fake_gateway import FakePaymentGateway
from payment.adapters.messaging.publisher import FastStreamPublisher
from payment.adapters.persistence.database import make_engine, make_sessionmaker
from payment.adapters.persistence.unit_of_work import SqlAlchemyUnitOfWork
from payment.adapters.webhook.http_sender import HttpxWebhookSender
from payment.application.services.outbox_service import (
    PAYMENT_CREATED_EVENT,
    OutboxService,
)
from payment.application.services.payment_service import PaymentService
from payment.application.services.webhook_service import WebhookService
from payment.config import Settings, get_settings
from payment.domain.services.retry_policy import ExponentialBackoffRetryPolicy


def make_channel(settings: Settings) -> Channel:
    """Канал брокера: prefetch и confirms задаются здесь, а не в подписчике."""
    return Channel(
        prefetch_count=settings.consumer_prefetch,
        publisher_confirms=True,
        on_return_raises=True,
    )


def make_broker(settings: Settings) -> RabbitBroker:
    """Брокер с явным каналом.

    Prefetch задаётся на канале FastStream (`Channel`), а не через
    `consume_args` подписчика (тот аргумент не доходил до aio-pika QoS).
    `on_return_raises=True` превращает возврат mandatory-сообщения (нет
    очереди) в `DeliveryError`, чтобы outbox не помечал запись отправленной.
    """
    return RabbitBroker(
        settings.rabbitmq_url,
        default_channel=make_channel(settings),
    )


class Container:
    """Собирает инфраструктуру; сервисы создаются на запрос/сообщение."""

    def __init__(
        self,
        *,
        settings: Settings | None = None,
    ) -> None:
        self.settings = settings or get_settings()
        self.retry_policy = ExponentialBackoffRetryPolicy(
            max_attempts=self.settings.max_attempts,
            base_delay=self.settings.retry_base_delay,
            factor=self.settings.retry_factor,
        )
        self.engine = make_engine(self.settings.database_url)
        self.sessionmaker: async_sessionmaker[AsyncSession] = make_sessionmaker(
            self.engine
        )
        self.gateway = FakePaymentGateway(
            min_seconds=self.settings.process_min_seconds,
            max_seconds=self.settings.process_max_seconds,
            success_rate=self.settings.success_rate,
        )
        self.webhook_sender = HttpxWebhookSender(
            secret=self.settings.webhook_secret,
            timeout_seconds=self.settings.webhook_timeout,
            allow_private_hosts=self.settings.webhook_allow_private_hosts,
        )
        self._broker: RabbitBroker | None = None

    def uow(self) -> SqlAlchemyUnitOfWork:
        """Свежий UoW на операцию: сессия создаётся при входе в контекст.

        Экземпляр не переиспользуется между запросами и сообщениями —
        иначе транзакции смешались бы.
        """
        return SqlAlchemyUnitOfWork(sessionmaker=self.sessionmaker)

    def payment_service(self) -> PaymentService:
        """Сервис запросов и обработки платежа с отдельным UoW на экземпляр."""
        return PaymentService(
            uow=self.uow(),
            gateway=self.gateway,
            claim_ttl_seconds=self.settings.claim_ttl_seconds,
            claim_wait_seconds=self.settings.claim_wait_seconds,
            claim_poll_seconds=self.settings.claim_poll_seconds,
        )

    def webhook_service(self) -> WebhookService:
        """Сервис доставки уведомлений; шлюз платежей здесь не вызывается."""
        return WebhookService(
            uow=self.uow(),
            sender=self.webhook_sender,
            claim_ttl_seconds=self.settings.webhook_claim_ttl_seconds,
            max_attempts=self.settings.max_attempts,
        )

    def outbox_service(self) -> OutboxService:
        """Сервис relay: маршрутизирует события outbox по очередям."""
        return OutboxService(
            uow=self.uow(),
            publisher=self.get_broker_publisher(),
            routes={PAYMENT_CREATED_EVENT: self.settings.rabbitmq_queue},
            subject=self.settings.rabbitmq_queue,
            max_attempts=self.settings.outbox_max_attempts,
        )

    def get_broker(self) -> RabbitBroker:
        if self._broker is None:
            self._broker = make_broker(self.settings)
        return self._broker

    def get_broker_publisher(self) -> FastStreamPublisher:
        return FastStreamPublisher(broker=self.get_broker())

    async def dispose(self) -> None:
        if self._broker is not None:
            await self._broker.stop()
        await self.engine.dispose()
