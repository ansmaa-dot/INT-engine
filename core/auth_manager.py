"""
Resolves credentials for any HTTP-facing node (destination, poller, webhook
verification). Profiles are loaded once at startup from config/auth_profiles.json
(git-ignored) or environment variables — NEVER from the channel definitions in
the database, so channel configs stay safe to inspect/export without leaking
secrets. Channel rows only ever store an `auth_profile_id` reference.
"""
import base64
import json
import os
import threading
import time
from dataclasses import dataclass, field
from typing import Optional

import requests


@dataclass
class AuthProfile:
    id: str
    type: str  # "none" | "api_key" | "basic" | "bearer_static" | "oauth2_client_credentials"
    header_name: Optional[str] = None
    api_key: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    token: Optional[str] = None
    token_url: Optional[str] = None
    client_id: Optional[str] = None
    client_secret: Optional[str] = None
    scope: Optional[str] = None

    @classmethod
    def from_dict(cls, d: dict) -> "AuthProfile":
        return cls(**{k: d.get(k) for k in cls.__dataclass_fields__})


class _CachedToken:
    __slots__ = ("token", "expires_at")

    def __init__(self, token: str, expires_at: float):
        self.token = token
        self.expires_at = expires_at

    def is_valid(self, skew_s: int = 60) -> bool:
        return time.time() < (self.expires_at - skew_s)


class AuthManager:
    def __init__(self, profiles: Optional[dict] = None):
        self._profiles: dict[str, AuthProfile] = profiles or {}
        self._token_cache: dict[str, _CachedToken] = {}
        self._locks: dict[str, threading.Lock] = {}
        self._locks_guard = threading.Lock()

    @classmethod
    def from_file(cls, path: str = "config/auth_profiles.json") -> "AuthManager":
        profiles = {}
        if os.path.exists(path):
            with open(path, "r") as f:
                raw = json.load(f)
            for entry in raw.get("profiles", []):
                p = AuthProfile.from_dict(entry)
                profiles[p.id] = p
        return cls(profiles)

    def list_profile_ids(self) -> list[str]:
        return list(self._profiles.keys())

    def has_profile(self, profile_id: str) -> bool:
        return profile_id in self._profiles

    def _lock_for(self, profile_id: str) -> threading.Lock:
        with self._locks_guard:
            if profile_id not in self._locks:
                self._locks[profile_id] = threading.Lock()
            return self._locks[profile_id]

    def invalidate(self, profile_id: str) -> None:
        self._token_cache.pop(profile_id, None)

    def get_headers(self, profile_id: Optional[str]) -> dict:
        if not profile_id:
            return {}
        profile = self._profiles.get(profile_id)
        if profile is None:
            raise ValueError(f"unknown auth profile: {profile_id}")

        if profile.type == "none":
            return {}
        if profile.type == "api_key":
            return {profile.header_name or "X-API-Key": profile.api_key}
        if profile.type == "basic":
            raw = f"{profile.username}:{profile.password}".encode()
            return {"Authorization": f"Basic {base64.b64encode(raw).decode()}"}
        if profile.type == "bearer_static":
            return {"Authorization": f"Bearer {profile.token}"}
        if profile.type == "oauth2_client_credentials":
            token = self._get_oauth_token(profile)
            return {"Authorization": f"Bearer {token}"}
        raise ValueError(f"unknown auth type: {profile.type}")

    def _get_oauth_token(self, profile: AuthProfile) -> str:
        # one lock per profile: prevents a burst of concurrent requests all
        # hitting an expired token from each firing their own refresh call
        lock = self._lock_for(profile.id)
        with lock:
            cached = self._token_cache.get(profile.id)
            if cached and cached.is_valid():
                return cached.token

            data = {
                "grant_type": "client_credentials",
                "client_id": profile.client_id,
                "client_secret": profile.client_secret,
            }
            if profile.scope:
                data["scope"] = profile.scope

            resp = requests.post(profile.token_url, data=data, timeout=10)
            resp.raise_for_status()
            body = resp.json()

            token = body["access_token"]
            expires_in = body.get("expires_in", 3600)
            self._token_cache[profile.id] = _CachedToken(token, time.time() + expires_in)
            return token
