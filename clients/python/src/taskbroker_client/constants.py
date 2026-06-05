from enum import Enum

DEFAULT_PROCESSING_DEADLINE = 10
"""
The fallback/default processing_deadline that tasks
will use if neither the TaskNamespace or Task define a deadline
"""

DEFAULT_REBALANCE_AFTER = 32
"""
The number of tasks a worker will process before it
selects a new broker instance.
"""

DEFAULT_CONSECUTIVE_UNAVAILABLE_ERRORS = 3
"""
The number of consecutive unavailable errors before the worker will
stop trying to connect to the broker and choose a new one.
"""

DEFAULT_TEMPORARY_UNAVAILABLE_HOST_TIMEOUT = 20
"""
The number of seconds to wait before a host is considered available again.
"""

DEFAULT_WORKER_QUEUE_SIZE = 5
"""
The size of multiprocessing.Queue used to communicate
with child processes.
"""

DEFAULT_CHILD_TASK_COUNT = 10000
"""
The number of tasks a worker child process will process
before being restarted.
"""

MAX_BACKOFF_SECONDS_WHEN_HOST_UNAVAILABLE = 20
"""
The maximum number of seconds to wait before retrying RPCs when the host is unavailable.
"""


MAX_PARAMETER_BYTES_BEFORE_COMPRESSION = 3000000  # 3MB
"""
The maximum number of bytes before a task parameter is compressed.
"""

DEFAULT_GRPC_MAX_MESSAGE_SIZE = 10 * 1024 * 1024  # 10MB
"""
The maximum size of a gRPC message in bytes.
Can be overridden via TASKBROKER_GRPC_MAX_MESSAGE_SIZE env var.
"""

DEFAULT_WORKER_HEALTH_CHECK_SEC_PER_TOUCH = 1.0
"""
The number of gRPC requests before touching the health check file
"""


ALWAYS_EAGER = False
"""
Whether or not tasks should be invoked eagerly (synchronously)
This can be mutated by application test harnesses to run tasks without Kafka.
"""

WORKER_CHILD_JOIN_TIMEOUT_SEC = 5
"""
How long the parent worker process should allow child processes
to drain pending produce futures on shutdown before sending SIGKILL.
"""

TASK_PRODUCER_MAX_PENDING_FUTURES = 10_000
"""
Maximum number of pending futures that can be in the TaskProducer module's
`_pending_futures` list. This list is a global, so is shared between all instances
of TaskProducer.
"""


class CompressionType(Enum):
    """
    The type of compression used for task parameters.
    """

    ZSTD = "zstd"
    PLAINTEXT = "plaintext"


class ParametersFormat(Enum):
    """
    How the producer populates the legacy `parameters` (JSON) and new
    `parameters_bytes` (msgpack) fields on TaskActivation.

    Set via env var `TASKBROKER_CLIENT_PARAMETERS_FORMAT`. Default BOTH.
    """

    BOTH = "both"
    JSON = "json"
    MSGPACK = "msgpack"
