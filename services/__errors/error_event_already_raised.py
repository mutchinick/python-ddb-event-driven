from services.__errors.error_base import ErrorBase
from services.__events.event_base import EventBase


class ErrorEventAlreadyRaisedException(ErrorBase):
    """
    Specific error for when an event with a given idempotency key
    has already been processed.

    This error is not transient by default.
    """

    def __init__(self, cause: Exception, event: EventBase):
        message = (
            f"Event '{event.eventName}' with idempotency key '{event.idempotencyKey}' has already been raised. "
            f"Cause: {str(cause)}"
        )

        super().__init__(message, is_transient=False)

        self.cause = cause
        self.event = event
