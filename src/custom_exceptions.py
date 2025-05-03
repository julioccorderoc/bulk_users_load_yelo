from typing import Any


class ApiClientError(Exception):
    """Base exception for API client errors."""

    def __init__(
        self,
        message: str,
        status_code: int | None,
        response_body: Any | None,
    ):
        self.status_code = status_code
        self.response_body = response_body
        super().__init__(message)

    def __str__(self):
        base = super().__str__()
        details = [f"StatusCode: {self.status_code}"] if self.status_code else []
        if self.response_body:
            details.append(
                f"Response: {str(self.response_body)[:200]}{'...' if len(str(self.response_body)) > 200 else ''}"
            )
        return f"{base} ({', '.join(details)})" if details else base


class ApiTimeoutError(ApiClientError):
    """Exception for request timeouts."""

    pass


class ApiConnectionError(ApiClientError):
    """Exception for network connection errors."""

    pass


class ApiHttpError(ApiClientError):
    """Exception for HTTP errors (4xx, 5xx)."""

    pass


class ApiResponseValidationError(ApiClientError):
    """Exception for when the API response doesn't match the expected schema."""

    pass
