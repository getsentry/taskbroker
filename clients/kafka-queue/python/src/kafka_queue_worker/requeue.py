class RequeueException(Exception):
    """Result delivery failed; worker pool may re-queue the result."""
