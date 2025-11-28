from __future__ import annotations

from abc import abstractmethod
from collections.abc import Mapping
from typing import Protocol, runtime_checkable

Tags = Mapping[str, str]


@runtime_checkable
class MetricsBackend(Protocol):
    """
    An abstract class that defines the interface for metrics backends.
    """

    @abstractmethod
    def incr(
        self,
        name: str,
        value: int | float = 1,
        tags: Tags | None = None,
    ) -> None:
        """
        Increments a counter metric by a given value.
        """
        raise NotImplementedError

    @abstractmethod
    def gauge(self, name: str, value: int | float, tags: Tags | None = None) -> None:
        """
        Sets a gauge metric to the given value.
        """
        raise NotImplementedError

    @abstractmethod
    def distribution(
        self,
        name: str,
        value: int | float,
        tags: Tags | None = None,
        unit: str | None = None,
    ) -> None:
        """
        Records a distribution metric.
        """
        raise NotImplementedError


class NoOpMetricsBackend(MetricsBackend):
    """
    Default metrics backend that does not record anything.
    """

    def incr(
        self,
        name: str,
        value: int | float = 1,
        tags: Tags | None = None,
    ) -> None:
        pass

    def gauge(self, name: str, value: int | float, tags: Tags | None = None) -> None:
        pass

    def distribution(
        self,
        name: str,
        value: int | float,
        tags: Tags | None = None,
        unit: str | None = None,
    ) -> None:
        pass
