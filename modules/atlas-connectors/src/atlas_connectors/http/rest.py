import httpx

from atlas_connectors.http.auth import AuthStrategy


class RestConnector:
    def __init__(
        self,
        base_url: str,
        auth: AuthStrategy | None = None,
    ):
        self.base_url = base_url
        self.auth = auth

    def get(self, endpoint: str) -> dict:
        headers = self.auth.get_headers() if self.auth else {}

        response = httpx.get(
            f"{self.base_url}{endpoint}",
            headers=headers,
            timeout=60,
        )

        response.raise_for_status()

        return response.json()