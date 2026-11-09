"""REST API connector and standard Connector contract adapter."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import httpx
from atlas_core import Connector

from atlas_connectors.exceptions import (
    ConnectorAuthenticationError,
    ConnectorConfigurationError,
    ConnectorConnectionError,
    ConnectorExtractionError,
)
from atlas_connectors.http.auth import AuthStrategy


def _cleanup_part_file(part_path: Path) -> None:
    """Best-effort removal of an incomplete ``.part`` download artifact."""
    try:
        if part_path.exists():
            part_path.unlink()
    except OSError:
        pass


class RestConnector(Connector):
    """HTTP REST connector supporting direct ``.get()`` calls and the ``Connector`` contract."""

    def __init__(
        self,
        base_url: str,
        auth: AuthStrategy | None = None,
        *,
        endpoint: str = "",
        timeout: float = 60.0,
    ) -> None:
        cleaned_url = base_url.strip() if isinstance(base_url, str) else ""
        if not cleaned_url:
            raise ConnectorConfigurationError("RestConnector requires a non-empty 'base_url'.")
        if not cleaned_url.startswith(("http://", "https://")):
            raise ConnectorConfigurationError(
                f"RestConnector 'base_url' must start with http:// or https://, got '{base_url}'."
            )
        self.base_url = cleaned_url.rstrip("/")
        self.auth = auth
        self.endpoint = endpoint
        self.timeout = timeout

    def _build_url(self, endpoint: str = "") -> str:
        """Resolve the full request URL for ``endpoint`` (or ``self.endpoint``)."""
        target_ep = endpoint or self.endpoint
        if target_ep and not target_ep.startswith("/"):
            return f"{self.base_url}/{target_ep}"
        return f"{self.base_url}{target_ep}"

    @property
    def source_name(self) -> str:
        """Return the configured REST endpoint URL."""
        return self._build_url()

    def validate_connection(self) -> bool:
        """Validate that the REST base URL is well-formed and reachable if probed."""
        return bool(self.base_url.startswith(("http://", "https://")))

    def get(self, endpoint: str = "") -> Any:
        """Execute an HTTP GET request against ``endpoint`` and return parsed JSON."""
        headers = self.auth.get_headers() if self.auth else {}
        url = self._build_url(endpoint)

        try:
            response = httpx.get(
                url,
                headers=headers,
                timeout=self.timeout,
            )
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as exc:
            status_code = exc.response.status_code
            if status_code in {401, 403}:
                raise ConnectorAuthenticationError(
                    f"Authentication failed with status {status_code} for '{url}'."
                ) from exc
            raise ConnectorExtractionError(
                f"HTTP request failed with status {status_code} for '{url}'."
            ) from exc
        except httpx.RequestError as exc:
            raise ConnectorConnectionError(
                f"Failed to connect to REST endpoint '{url}': {exc}"
            ) from exc
        except ValueError as exc:
            raise ConnectorExtractionError(
                f"Failed to decode JSON response from '{url}': {exc}"
            ) from exc

    def _stream_to_destination(self, destination: Path, endpoint: str = "") -> Path:
        """Stream an HTTP GET response to a ``.part`` file and atomically replace destination."""
        dest_path = destination if isinstance(destination, Path) else Path(destination)
        parent_dir = dest_path.parent
        if not parent_dir.exists() or not parent_dir.is_dir():
            raise ConnectorConfigurationError(
                f"Destination parent directory does not exist: '{parent_dir}'."
            )

        headers = self.auth.get_headers() if self.auth else {}
        url = self._build_url(endpoint)
        part_path = dest_path.with_suffix(dest_path.suffix + ".part")

        try:
            with httpx.stream(
                "GET",
                url,
                headers=headers,
                timeout=self.timeout,
            ) as response:
                response.raise_for_status()

                with part_path.open("wb") as file:
                    for chunk in response.iter_bytes():
                        if chunk:
                            file.write(chunk)

            part_path.replace(dest_path)
            return dest_path
        except httpx.HTTPStatusError as exc:
            _cleanup_part_file(part_path)
            status_code = exc.response.status_code
            if status_code in {401, 403}:
                raise ConnectorAuthenticationError(
                    f"Authentication failed with status {status_code} for '{url}'."
                ) from exc
            raise ConnectorExtractionError(
                f"HTTP request failed with status {status_code} for '{url}'."
            ) from exc
        except (
            httpx.ReadTimeout,
            httpx.ConnectTimeout,
            httpx.ConnectError,
            httpx.RequestError,
        ) as exc:
            _cleanup_part_file(part_path)
            raise ConnectorConnectionError(
                f"Failed to stream from REST endpoint '{url}': {exc}"
            ) from exc
        except OSError as exc:
            _cleanup_part_file(part_path)
            raise ConnectorExtractionError(
                f"Filesystem error while writing download to '{dest_path}': {exc}"
            ) from exc
        except Exception:
            _cleanup_part_file(part_path)
            raise

    def extract(
        self,
        *,
        destination: Path | None = None,
        **kwargs: Any,
    ) -> Any:
        """Execute extraction through the standard :class:`Connector` contract."""
        endpoint = str(kwargs.get("endpoint", self.endpoint))
        if destination is not None:
            return self._stream_to_destination(destination, endpoint=endpoint)
        return self.get(endpoint)


class RestConnectorAdapter(Connector):
    """Adapter wrapping :class:`RestConnector` to expose the standard :class:`Connector` API."""

    def __init__(
        self,
        connector: RestConnector | None = None,
        *,
        base_url: str | None = None,
        auth: AuthStrategy | None = None,
        endpoint: str = "",
        timeout: float = 60.0,
    ) -> None:
        if connector is not None:
            self._connector = connector
        elif base_url is not None:
            self._connector = RestConnector(
                base_url=base_url,
                auth=auth,
                endpoint=endpoint,
                timeout=timeout,
            )
        else:
            raise ConnectorConfigurationError(
                "RestConnectorAdapter requires either a 'connector' instance or 'base_url'."
            )
        self._default_endpoint = endpoint or self._connector.endpoint

    @property
    def connector(self) -> RestConnector:
        """Return the wrapped :class:`RestConnector`."""
        return self._connector

    @property
    def source_name(self) -> str:
        """Return the source identifier for the wrapped REST connector."""
        if self._default_endpoint:
            ep = (
                self._default_endpoint
                if self._default_endpoint.startswith("/")
                else f"/{self._default_endpoint}"
            )
            return f"{self._connector.base_url}{ep}"
        return self._connector.source_name

    def validate_connection(self) -> bool:
        """Validate the wrapped REST connector configuration."""
        return self._connector.validate_connection()

    def extract(
        self,
        *,
        destination: Path | None = None,
        **kwargs: Any,
    ) -> Any:
        """Delegate extraction to the wrapped :class:`RestConnector`."""
        endpoint = str(kwargs.get("endpoint", self._default_endpoint))
        return self._connector.extract(destination=destination, endpoint=endpoint)


__all__ = [
    "RestConnector",
    "RestConnectorAdapter",
]
