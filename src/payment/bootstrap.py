"""Composition root: единственное место сборки зависимостей."""

from typing import Optional

from faststream.rabbit import RabbitBroker
from sqlalchemy.ext.asyncio import async_sessionmaker

from payment.adapters.gateway.fake_gateway import FakePaymentGateway
from payment.adapters.messaging.publisher import FastStreamPublisher
from payment.adapters.persistence.database import make_engine, make_sessionmaker
from payment.adapters.persistence.unit_of_work import SqlAlchemyUnitOfWork
from payment.adapters.webhook.http_sender import HttpxWebhookSender
from payment.adapters.webhook.signer import sign_payload
from payment.application.use_cases.create_payment import CreatePayment
from payment.application.use_cases.get_payment import GetPayment
from payment.application.use_cases.process_payment import ProcessPayment
from payment.application.use_cases.publish_outbox import PublishOutbox
from payment.config import Settings, get_settings


class Container:
    """Собирает инфраструктуру; use case'ы создаются на запрос/сообщение."""

    def __init__(self, settings: Optional[Settings] = None) -> None:
        self.settings = settings or get_settings()
        self.engine = make_engine(self.settings.database_url)
        self.sessionmaker: async_sessionmaker = make_sessionmaker(self.engine)
        self.gateway = FakePaymentGateway(
            min_seconds=self.settings.process_min_seconds,
            max_seconds=self.settings.process_max_seconds,
            success_rate=self.settings.success_rate,
        )
        self.webhook_sender = HttpxWebhookSender(timeout_seconds=self.settings.webhook_timeout)
        self._broker: Optional[RabbitBroker] = None

    def uow(self) -> SqlAlchemyUnitOfWork:
        return SqlAlchemyUnitOfWork(self.sessionmaker)

    def create_payment(self) -> CreatePayment:
        return CreatePayment(uow=self.uow())

    def get_payment(self) -> GetPayment:
        return GetPayment(uow=self.uow())

    def process_payment(self) -> ProcessPayment:
        return ProcessPayment(
            uow=self.uow(),
            gateway=self.gateway,
            webhook_sender=self.webhook_sender,
            sign=self.sign,
        )

    def publish_outbox(self) -> PublishOutbox:
        return PublishOutbox(uow=self.uow(), publisher=self.get_broker_publisher())

    def get_broker(self) -> RabbitBroker:
        if self._broker is None:
            self._broker = RabbitBroker(self.settings.rabbitmq_url)
        return self._broker

    def get_broker_publisher(self) -> FastStreamPublisher:
        return FastStreamPublisher(self.get_broker())

    def sign(self, payload: dict) -> str:
        return sign_payload(self.settings.webhook_secret, payload)

    async def dispose(self) -> None:
        if self._broker is not None:
            await self._broker.stop()
        await self.engine.dispose()
