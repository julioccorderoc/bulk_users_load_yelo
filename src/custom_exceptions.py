from typing import Any


class ApiClientError(Exception):
    """Base exception for API client errors."""

    def __init__(
        self,
        message: str,
        status_code: int | None = None,
        response_body: Any | None = None,
    ):
        self.status_code = status_code
        self.response_body = response_body
        super().__init__(message)

    def __str__(self):
        details = []
        if self.request_url:
            details.append(f"URL: {self.request_url}")
        if self.status_code:
            details.append(f"StatusCode: {self.status_code}")
        if self.response_body:
            body_str = str(self.response_body)
            details.append(
                f"Response: {body_str[:200]}{'...' if len(body_str) > 200 else ''}"
            )

        base_message = super().__str__()
        return f"{base_message} ({', '.join(details)})" if details else base_message


class ApiTimeoutError(ApiClientError):
    """Exception for request timeouts."""

    pass


class ApiConnectionError(ApiClientError):
    """Exception for network connection errors."""

    pass


class ApiHttpError(ApiClientError):
    """Exception for HTTP errors (4xx, 5xx)."""

    def __init__(
        self,
        message: str,
        status_code: int,
        response_body: Any,
        request_url: str | None = None,
    ):
        super().__init__(
            message=message,
            status_code=status_code,
            response_body=response_body,
            request_url=request_url,
        )


class ApiResponseValidationError(ApiClientError):
    """Exception for when the API response doesn't match the expected schema."""

    def __init__(
        self,
        message: str,
        status_code: int | None,
        response_body: Any,
        request_url: str | None = None,
    ):
        super().__init__(
            message=message,
            status_code=status_code,
            response_body=response_body,
            request_url=request_url,
        )
