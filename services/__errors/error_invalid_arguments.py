from services.__errors.error_base import ErrorBase


class ErrorInvalidArgumentsException(ErrorBase):
    """
    Specific error for when invalid arguments are provided to a function or method.

    This error is not transient by default.
    """

    def __init__(self, cause: Exception):
        message = f"ErrorInvalidArgumentsException: Cause: {str(cause)}"

        super().__init__(message, is_transient=False)

        self.cause = cause
