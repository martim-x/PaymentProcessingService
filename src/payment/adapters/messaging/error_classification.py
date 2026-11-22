"""Классификация сбоев обработчика: транзиентная инфраструктура vs бизнес-отказ.

Зачем: обработчик платежа не может считать любое исключение попыткой обработки.
Упавшее соединение с БД или брокером — не отказ платежа, ему нужна отдельная,
более терпеливая цепочка ретраев (`x-infra-attempt`). Бизнес-отказ (платёж занят,
не найден, отказ шлюза) — обычная попытка обработки (`x-attempt`).

Список намеренно узкий: он знает только про известные транзиентные ошибки
(SQLAlchemy/psycopg/aio-pika/asyncio) и не считает транзиентной, например,
`IntegrityError` — это конфликт данных, ретрай его не вылечит.
"""

import asyncio
from functools import lru_cache

from sqlalchemy.exc import DisconnectionError, InterfaceError, OperationalError
from sqlalchemy.exc import TimeoutError as PoolTimeoutError

from payment.application.ports.message_publisher import PublisherUnavailable


class InfrastructureErrorClassifier:
    """Классифицирует известные транзиентные ошибки инфраструктуры."""

    _max_cause_depth = 3

    @staticmethod
    @lru_cache(maxsize=1)
    def _transient_types() -> tuple[type[BaseException], ...]:
        """Известные транзиентные ошибки; опциональные драйверы подключаются лениво."""
        types: list[type[BaseException]] = [
            OperationalError,
            InterfaceError,
            DisconnectionError,
            PoolTimeoutError,
            PublisherUnavailable,
            ConnectionError,
            TimeoutError,
            asyncio.TimeoutError,
        ]
        try:
            import psycopg
        except ImportError:  # pragma: no cover — psycopg обязателен в проде
            pass
        else:
            types.extend([psycopg.OperationalError, psycopg.InterfaceError])
        try:
            from aio_pika.exceptions import AMQPConnectionError
        except ImportError:  # pragma: no cover — aio-pika приходит с faststream[rabbit]
            pass
        else:
            types.append(AMQPConnectionError)
        return tuple(dict.fromkeys(types))

    @classmethod
    def is_transient_infra_error(cls, exc: BaseException) -> bool:
        """True, если сбой вызван недоступностью БД/транспорта и стоит ретраить отдельно.

        Проверяется и само исключение, и его причина (`__cause__`/`__context__`):
        сервис сценария может обернуть `OperationalError` в собственное исключение.
        """
        current: BaseException | None = exc
        for _ in range(cls._max_cause_depth + 1):
            if current is None:
                return False
            if isinstance(current, cls._transient_types()):
                return True
            current = current.__cause__ or current.__context__
        return False
