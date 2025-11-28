from __future__ import annotations

from abc import abstractmethod
from collections.abc import Mapping
from contextlib import contextmanager
from typing import Generator, Protocol, runtime_checkable

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
        sample_rate: float | None = None,
    ) -> None:
        """
        Increments a counter metric by a given value.
        """
        raise NotImplementedError

    @abstractmethod
    def distribution(
        self,
        name: str,
        value: int | float,
        tags: Tags | None = None,
        unit: str | None = None,
        sample_rate: float | None = None,
    ) -> None:
        """
        Records a distribution metric.
        """
        raise NotImplementedError

    @contextmanager
    def timer(
        self,
        key: str,
        instance: str | None = None,
        tags: Tags | None = None,
        sample_rate: float | None = None,
        stacklevel: int = 0,
    ) -> Generator[None]:
        """
        Records a distribution metric with a context manager.
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
        sample_rate: float | None = None,
    ) -> None:
        pass

    def distribution(
        self,
        name: str,
        value: int | float,
        tags: Tags | None = None,
        unit: str | None = None,
        sample_rate: float | None = None,
    ) -> None:
        pass

    @contextmanager
    def timer(
        self,
        key: str,
        instance: str | None = None,
        tags: Tags | None = None,
        sample_rate: float | None = None,
        stacklevel: int = 0,
    ) -> Generator[None]:
        yield None
