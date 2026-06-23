from enum import Enum


class TraceEvent(Enum):
    WORKER_STARTED = "WORKER_UP"
    RECEIVED = "RECEIVED"
    TIMED_OUT = "TIMED_OUT"
    QUEUED = "QUEUED"
    PICKED_UP = "PICKED_UP"
    WORKER_STOPPED = "WORKER_STOPPED"
