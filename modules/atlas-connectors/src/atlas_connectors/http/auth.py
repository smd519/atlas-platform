from abc import ABC, abstractmethod


class AuthStrategy(ABC):

    @abstractmethod
    def get_headers(self) -> dict[str, str]:
        pass


class ApiKeyAuth(AuthStrategy):
    def __init__(
        self,
        header_name: str,
        api_key: str,
    ):
        self._header_name = header_name
        self._api_key = api_key

    def get_headers(self) -> dict[str, str]:
        return {
            self._header_name: self._api_key,
        }


class BearerTokenAuth(AuthStrategy):
    def __init__(self, token: str):
        self._token = token

    def get_headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self._token}",
        }