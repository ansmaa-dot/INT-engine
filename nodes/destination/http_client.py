import requests

from nodes.base import DestinationNode
from core.auth_manager import AuthManager


class HttpClientNode(DestinationNode):
    def __init__(self, endpoint_url: str, method: str = "POST",
                 auth: AuthManager | None = None, auth_profile_id: str | None = None,
                 headers: dict = None, timeout: int = 5):
        self.endpoint_url = endpoint_url
        self.method = method.upper()
        self.auth = auth
        self.auth_profile_id = auth_profile_id
        self.extra_headers = headers or {}
        self.timeout = timeout

    def send(self, payload: dict):
        headers = {"Content-Type": "application/json", **self.extra_headers}
        if self.auth and self.auth_profile_id:
            headers.update(self.auth.get_headers(self.auth_profile_id))

        response = requests.request(
            self.method, self.endpoint_url, json=payload, headers=headers,
            timeout=self.timeout,
        )

        if response.status_code == 401 and self.auth and self.auth_profile_id:
            # cached token may be stale (revoked, rotated, clock skew) —
            # invalidate and retry exactly once with a fresh token
            self.auth.invalidate(self.auth_profile_id)
            headers.update(self.auth.get_headers(self.auth_profile_id))
            response = requests.request(
                self.method, self.endpoint_url, json=payload, headers=headers,
                timeout=self.timeout,
            )

        response.raise_for_status()
        return response.status_code
