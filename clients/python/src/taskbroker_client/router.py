from typing import Protocol


class TaskRouter(Protocol):
    """
    Resolves task namespaces to a topic names.
    """

    def route_namespace(self, name: str) -> str: ...


class DefaultRouter(TaskRouter):
    """
    Stub router that resolves all namespaces to a default topic
    """

    def route_namespace(self, name: str) -> str:
        return "taskbroker"
