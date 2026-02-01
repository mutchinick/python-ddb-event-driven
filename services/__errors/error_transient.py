from services.__errors.error_base import ErrorBase


class ErrorTransient(ErrorBase):
    """
    Specific error for when a transient cause occurs.

    This error is transient by default.
    """

    def __init__(self, cause: Exception):
        message = f"ErrorTransientCauseException: Cause: {str(cause)}"

        super().__init__(message, is_transient=True)

        self.cause = cause
