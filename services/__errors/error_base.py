class ErrorBase(Exception):
    """
    A base class for custom application errors that includes a retry flag.
    """

    def __init__(self, message: str, is_transient: bool):
        self.is_transient = is_transient
        super().__init__(message)

    @staticmethod
    def safe_is_transient(error: Exception) -> bool:
        """
        Determines if the given error is transient based on its type.
        Defaults to True for unknown error types to optimize for operation completion.
        Most unknown errors (network issues, memory overflows, etc.) are transient.
        """
        if isinstance(error, ErrorBase):
            return error.is_transient
        return True
