from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class PollRequest(_message.Message):
    __slots__ = ("poll_timeout_ms",)
    POLL_TIMEOUT_MS_FIELD_NUMBER: _ClassVar[int]
    poll_timeout_ms: int
    def __init__(self, poll_timeout_ms: _Optional[int] = ...) -> None: ...

class PollResponse(_message.Message):
    __slots__ = ("empty", "delivery_id", "payload", "bridge_host", "total_polls", "total_deliveries")
    EMPTY_FIELD_NUMBER: _ClassVar[int]
    DELIVERY_ID_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    BRIDGE_HOST_FIELD_NUMBER: _ClassVar[int]
    TOTAL_POLLS_FIELD_NUMBER: _ClassVar[int]
    TOTAL_DELIVERIES_FIELD_NUMBER: _ClassVar[int]
    empty: bool
    delivery_id: str
    payload: bytes
    bridge_host: str
    total_polls: int
    total_deliveries: int
    def __init__(self, empty: bool = ..., delivery_id: _Optional[str] = ..., payload: _Optional[bytes] = ..., bridge_host: _Optional[str] = ..., total_polls: _Optional[int] = ..., total_deliveries: _Optional[int] = ...) -> None: ...

class CompleteRequest(_message.Message):
    __slots__ = ("delivery_id", "task_id", "activation_status", "fetch_next", "poll_timeout_ms")
    DELIVERY_ID_FIELD_NUMBER: _ClassVar[int]
    TASK_ID_FIELD_NUMBER: _ClassVar[int]
    ACTIVATION_STATUS_FIELD_NUMBER: _ClassVar[int]
    FETCH_NEXT_FIELD_NUMBER: _ClassVar[int]
    POLL_TIMEOUT_MS_FIELD_NUMBER: _ClassVar[int]
    delivery_id: str
    task_id: str
    activation_status: int
    fetch_next: bool
    poll_timeout_ms: int
    def __init__(self, delivery_id: _Optional[str] = ..., task_id: _Optional[str] = ..., activation_status: _Optional[int] = ..., fetch_next: bool = ..., poll_timeout_ms: _Optional[int] = ...) -> None: ...

class CompleteResponse(_message.Message):
    __slots__ = ("has_next", "next_delivery_id", "next_payload", "bridge_host", "total_completes")
    HAS_NEXT_FIELD_NUMBER: _ClassVar[int]
    NEXT_DELIVERY_ID_FIELD_NUMBER: _ClassVar[int]
    NEXT_PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    BRIDGE_HOST_FIELD_NUMBER: _ClassVar[int]
    TOTAL_COMPLETES_FIELD_NUMBER: _ClassVar[int]
    has_next: bool
    next_delivery_id: str
    next_payload: bytes
    bridge_host: str
    total_completes: int
    def __init__(self, has_next: bool = ..., next_delivery_id: _Optional[str] = ..., next_payload: _Optional[bytes] = ..., bridge_host: _Optional[str] = ..., total_completes: _Optional[int] = ...) -> None: ...

class CheckRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class CheckResponse(_message.Message):
    __slots__ = ("ready", "detail")
    READY_FIELD_NUMBER: _ClassVar[int]
    DETAIL_FIELD_NUMBER: _ClassVar[int]
    ready: bool
    detail: str
    def __init__(self, ready: bool = ..., detail: _Optional[str] = ...) -> None: ...
