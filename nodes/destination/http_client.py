import requests

from nodes.base import DestinationNode
from core.auth_manager import AuthManager
from core.transport import DestinationMessage


class HttpClientNode(DestinationNode):
    def __init__(self, endpoint_url: str, method: str = "POST",
                 auth: AuthManager | None = None, auth_profile_id: str | None = None,
                 headers: dict = None, timeout: int = 5,
                 default_content_type: str = "application/json"):
        self.endpoint_url = endpoint_url
        self.method = method.upper()
        self.auth = auth
        self.auth_profile_id = auth_profile_id
        self.extra_headers = headers or {}
        self.timeout = timeout
        self.default_content_type = default_content_type

    def send(self, message: DestinationMessage):
        # The destination receives already-serialized content; it never
        # serializes arbitrary dicts. Content-Type comes from the message or
        # a default, and may be overridden by configured headers.
        content = message.content
        if isinstance(content, str):
            body = content.encode("utf-8")
        elif isinstance(content, bytes):
            body = content
        else:
            raise TypeError(
                "destination content must be str/bytes; destination does not serialize"
            )

        headers = {
            "Content-Type": message.content_type or self.default_content_type,
            **self.extra_headers,
        }
        if self.auth and self.auth_profile_id:
            headers.update(self.auth.get_headers(self.auth_profile_id))

        response = requests.request(
            self.method, self.endpoint_url, headers=headers,
            timeout=self.timeout, data=body,
        )

        if response.status_code == 401 and self.auth and self.auth_profile_id:
            # cached token may be stale (revoked, rotated, clock skew) —
            # invalidate and retry exactly once with a fresh token
            self.auth.invalidate(self.auth_profile_id)
            headers.update(self.auth.get_headers(self.auth_profile_id))
            response = requests.request(
                self.method, self.endpoint_url, headers=headers,
                timeout=self.timeout, data=body,
            )

        response.raise_for_status()
        return response.status_code
