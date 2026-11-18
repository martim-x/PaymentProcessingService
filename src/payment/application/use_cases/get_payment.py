"""Use case: получение платежа (API)."""

from typing import Optional

from payment.application.dto import PaymentDetails
from payment.application.exceptions import PaymentNotFound
from payment.application.ports.repositories import AbstractUnitOfWork
from payment.domain.model.payment import Payment
from payment.domain.model.value_objects import PaymentId


class GetPayment:
    def __init__(self, *, uow: AbstractUnitOfWork) -> None:
        self._uow = uow

    async def execute(self, payment_id: PaymentId) -> PaymentDetails:
        payment: Optional[Payment] = None
        async with self._uow as uow:
            payment = await uow.payments.get(payment_id)
        if payment is None:
            raise PaymentNotFound(str(payment_id))
        return PaymentDetails.from_payment(payment)
