"""Политика повторных попыток (retry) с экспоненциальной задержкой."""

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ExponentialBackoffRetryPolicy:
    """Попытки нумеруются с 1: delay = base * factor ** (attempt - 1)."""

    max_attempts: int = 3
    base_delay: float = 1.0
    factor: float = 2.0

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        if self.base_delay < 0 or self.factor <= 0:
            raise ValueError("base_delay must be >= 0 and factor > 0")

    def is_exhausted(self, *, attempts: int) -> bool:
        """True, если исчерпаны все попытки (дальше ретраить нельзя)."""
        return attempts >= self.max_attempts

    def delay_for_attempt(self, attempt: int) -> float:
        """Задержка перед указанной попыткой (1-индексация)."""
        if attempt < 1:
            raise ValueError("attempt starts at 1")
        return self.base_delay * (self.factor ** (attempt - 1))
