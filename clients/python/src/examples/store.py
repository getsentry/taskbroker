from taskbroker_client.types import AtMostOnceStore

class StubAtMostOnce(AtMostOnceStore):
    def __init__(self) -> None:
        self._keys: dict[str, str] = {}

    def add(self, key: str, value: str, timeout: int) -> bool:
        if key in self._keys:
            return False
        self._keys[key] = value
        return True
