from typing import Protocol


class TaskRouter(Protocol):
    def route_namespace(self, name: str) -> str: ...
