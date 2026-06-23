from enum import Enum


class ActivationEvent(Enum):
    RECEIVED = "RECEIVED"
    TIMED_OUT = "TIMED_OUT"
    QUEUED = "QUEUED"
    PICKED_UP = "PICKED_UP"
