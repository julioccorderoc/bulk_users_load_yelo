import httpx
import os
from typing import Any, Type

from pydantic import (
    BaseModel,
    ValidationError,
)
from dotenv import load_dotenv

from custom_exceptions import (
    ApiClientError,
    ApiTimeoutError,
    ApiConnectionError,
    ApiHttpError,
    ApiResponseValidationError,
)
from utils import yelo_headers, logger


# --- Environment Variables ---
load_dotenv()
YELO_API_BASE_URL = os.getenv("YELO_API_BASE_URL")
DEFAULT_TIMEOUT = os.getenv("DEFAULT_TIMEOUT")
YELO_API_TOKEN = os.getenv("YELO_API_TOKEN")


# --- API Client Class ---
class YeloApiClient:
    """
    A base client for interacting with the Yelo REST API asynchronously.

    Handles request construction, execution, basic error handling, and response parsing.
    """

    def __init__(
        self,
        base_url: str = YELO_API_BASE_URL,
        request_headers: dict[str, str] = yelo_headers,
        auth_token: str | None = None,  # Pass token during init or configure globally
        timeout: int = DEFAULT_TIMEOUT,
    ):
        """
        Initializes the asynchronous API client.

        Args:
            base_url: The base URL for the Yelo API.
            auth_token: The API authentication token (e.g., Bearer token).
            request_headers: Dictionary of headers to include in every request.
            timeout: Default request timeout in seconds.
        """
        if not base_url.endswith("/"):
            base_url += "/"
        self.base_url = base_url
        self.timeout = timeout

        if auth_token:
            request_headers["Authorization"] = f"Bearer {auth_token}"

        # Use httpx.AsyncClient for persistent connections and async support
        self._client = httpx.AsyncClient(
            base_url=self.base_url,
            headers=request_headers,
            timeout=self.timeout,
            # Consider enabling HTTP/2 if the server supports it for potential performance gains
            # http2=True,
            event_hooks={
                "request": [self._log_request],
                "response": [self._log_response],
            },
        )
        logger.info(f"YeloApiClient initialized for base URL: {self.base_url}")

    async def close(self):
        """Gracefully close the underlying httpx client and connections."""
        await self._client.aclose()
        logger.info("YeloApiClient connection closed.")

    # --- Logging Hooks ---
    async def _log_request(self, request: httpx.Request):
        headers_to_log = {
            k: v for k, v in request.headers.items() if k.lower() != "authorization"
        }
        logger.debug(f"--> {request.method} {request.url}")
        logger.debug(f"    Headers: {headers_to_log}")
        content = (
            await request.aread()
        )  # read content if needed, careful with large bodies
        if content:
            logger.debug(f"Body: {content.decode()}")
        return

    async def _log_response(self, response: httpx.Response):
        # Ensure response stream is read before logging body if necessary
        # Use await response.aread() before accessing response.text or response.json
        # This happens automatically if accessing .text or .json but good to be aware of
        request = response.request
        logger.debug(
            f"<-- {request.method} {request.url} - Status {response.status_code}"
        )
        try:
            logger.debug(f"Response Body: {response.text}")
        except Exception:
            logger.debug("Response Body: (Could not decode or read)")
        return

    # --- Core Request Method ---
    async def _request(
        self,
        method: str,
        endpoint: str,
        payload: BaseModel | list[Type[BaseModel]] | list | dict | None = None,
        params: dict[str, Any] | None = None,
        expected_status: int = 200,  # Default expected success code for GET/PUT/DELETE
        response_model: Type[BaseModel]
        | None = None,  # Optional Pydantic model for response validation
    ) -> Any:
        """
        Internal method to make an API request.

        Args:
            method: HTTP method (GET, POST, PUT, DELETE, etc.).
            endpoint: API endpoint path (relative to base_url).
            payload: Data to send in the request body (usually for POST, PUT, PATCH).
                     If it's a Pydantic model, it will be serialized to dict.
            params: URL query parameters.
            expected_status: The HTTP status code expected for a successful operation.
                             Use 201 for resource creation (POST).
            response_model: Optional Pydantic model to validate the response structure.

        Returns:
            The parsed JSON response data (or raw response if JSON parsing fails but status is OK).
            If response_model is provided, returns an instance of that model.

        Raises:
            ApiTimeoutError: If the request times out.
            ApiConnectionError: If a network connection error occurs.
            ApiHttpError: If the API returns an unexpected HTTP status code (4xx, 5xx).
            ApiResponseValidationError: If response validation against response_model fails.
            ApiClientError: For other generic client-side errors.
        """
        # Ensure endpoint doesn't start with a slash if base_url ends with one
        endpoint = endpoint.lstrip("/")

        # Serialize Pydantic models if provided as payload
        json_payload = None
        if isinstance(payload, BaseModel):
            json_payload = payload.model_dump(mode="json")
        elif payload is not None:
            json_payload = payload  # Assume it's already JSON-serializable (dict, list)

        try:
            logger.info(f"Making {method} request to {endpoint}...")
            response = await self._client.request(
                method=method,
                url=endpoint,
                json=json_payload,  # `json` param handles serialization and content-type header
                params=params,
            )

            # --- Fail Fast on HTTP Errors ---
            # Check if the status code is what we expected or a general success code
            # Use raise_for_status() for standard HTTP error checking (4xx, 5xx)
            response.raise_for_status()  # Raises httpx.HTTPStatusError for 4xx/5xx

            # --- Optional: Stricter Check for Specific Success Code ---
            if response.status_code != expected_status:
                logger.error(
                    f"API Error: Expected status {expected_status}, got {response.status_code} for {method} {endpoint}"
                )
                raise ApiHttpError(
                    f"Unexpected status code: {response.status_code}",
                    status_code=response.status_code,
                    response_body=response.text,  # Include response body for debugging
                )

            # --- Process Successful Response ---
            # Handle cases with no content (e.g., 204 No Content)
            if response.status_code == 204 or not response.content:
                logger.info(
                    f"Request successful ({response.status_code}), no content returned."
                )
                # If a response model was expected but none given, return None
                if response_model:
                    logger.warning(
                        f"Expected response model {response_model.__name__} but got no content (204)."
                    )
                    # TODO: raise ApiResponseValidationError here
                return None

            # Attempt to parse JSON response
            try:
                json_response = response.json()
            except ValueError as e:
                # Handle cases where API returns non-JSON response despite success status
                logger.error(
                    f"Failed to decode JSON response for {method} {endpoint}: {e}. Response Text: {response.text[:200]}..."
                )
                raise ApiClientError(
                    "Failed to decode JSON response from API.",
                    status_code=response.status_code,
                    response_body=response.text,
                ) from e

            # --- Optional: Validate Response Structure ---
            if response_model:
                try:
                    validated_response = response_model.model_validate(json_response)
                    logger.debug(
                        f"Response validated successfully against {response_model.__name__}"
                    )
                    return validated_response
                except ValidationError as e:
                    logger.error(
                        f"API response validation failed for {method} {endpoint} against model {response_model.__name__}: {e}"
                    )
                    raise ApiResponseValidationError(
                        f"API response validation failed: {e}",
                        status_code=response.status_code,
                        response_body=json_response,
                    ) from e

            # Return raw JSON if no validation model provided
            return json_response

        # --- Specific Error Handling ---
        except httpx.TimeoutException as e:
            logger.error(
                f"API Timeout Error: Request to {e.request.url} timed out after {self.timeout}s."
            )
            raise ApiTimeoutError(f"Request timed out: {e.request.url}") from e
        except httpx.ConnectError as e:
            logger.error(
                f"API Connection Error: Could not connect to {e.request.url}. Error: {e}"
            )
            raise ApiConnectionError(f"Connection error: {e.request.url}") from e
        except httpx.HTTPStatusError as e:
            # This catches the error raised by response.raise_for_status()
            logger.error(
                f"API HTTP Error: {e.response.status_code} for {e.request.method} {e.request.url}"
            )
            try:
                response_body = e.response.json()
            except ValueError:
                response_body = e.response.text
            logger.error(f"API Error Body: {str(response_body)[:500]}...")
            raise ApiHttpError(
                f"HTTP error {e.response.status_code} for {e.request.url}",
                status_code=e.response.status_code,
                response_body=response_body,
            ) from e
        except httpx.RequestError as e:
            logger.error(
                f"API Request Error: An error occurred during the request to {e.request.url}. Error: {e}"
            )
            raise ApiClientError(f"Generic request error: {e.request.url}") from e

    # --- Public Convenience Methods ---
    async def get(
        self,
        endpoint: str,
        params: dict[str, Any] | None = None,
        response_model: Type[BaseModel] | None = None,
        expected_status: int = 200,
    ) -> Any:
        """Perform a GET request."""
        return await self._request(
            "GET",
            endpoint,
            params=params,
            expected_status=expected_status,
            response_model=response_model,
        )

    async def post(
        self,
        endpoint: str,
        payload: Any,
        params: dict[str, Any] | None = None,
        response_model: Type[BaseModel] | None = None,
        expected_status: int = 200,
    ) -> Any:
        """Perform a POST request (typically expects 201 Created)."""
        return await self._request(
            "POST",
            endpoint,
            payload=payload,
            params=params,
            expected_status=expected_status,
            response_model=response_model,
        )

    async def put(
        self,
        endpoint: str,
        payload: Any,
        params: dict[str, Any] | None = None,
        response_model: Type[BaseModel] | None = None,
        expected_status: int = 200,
    ) -> Any:
        """Perform a PUT request (typically expects 200 OK)."""
        return await self._request(
            "PUT",
            endpoint,
            payload=payload,
            params=params,
            expected_status=expected_status,
            response_model=response_model,
        )

    async def patch(
        self,
        endpoint: str,
        payload: Any,
        params: dict[str, Any] | None = None,
        response_model: Type[BaseModel] | None = None,
        expected_status: int = 200,
    ) -> Any:
        """Perform a PATCH request (typically expects 200 OK)."""
        return await self._request(
            "PATCH",
            endpoint,
            payload=payload,
            params=params,
            expected_status=expected_status,
            response_model=response_model,
        )

    async def delete(
        self,
        endpoint: str,
        params: dict[str, Any] | None = None,
        expected_status: int = 204,
    ) -> Any:
        """Perform a DELETE request (typically expects 204 No Content)."""
        # Note: Usually no response body or model for DELETE
        return await self._request(
            "DELETE",
            endpoint,
            params=params,
            expected_status=expected_status,
            response_model=None,
        )

    # --- Context Manager Support ---
    async def __aenter__(self):
        # Allows using 'async with YeloApiClient(...) as client:'
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        # Ensures the client is closed when exiting the 'async with' block
        await self.close()
