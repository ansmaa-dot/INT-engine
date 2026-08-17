import json
import os

from nodes.base import DestinationNode


class SFTPClientNode(DestinationNode):
    """SFTP destination: uploads each payload as a file to a remote SFTP
    server. Supports both password and private-key authentication.

    Payload handling:
      - dict with a "filename" key -> uses that as the remote filename
      - dict with a "content" key -> writes that content (str or bytes)
      - any other dict -> serialized to JSON
      - str/bytes -> written as-is
    """

    def __init__(self, host: str, port: int = 22, username: str = "",
                 password: str | None = None, private_key_path: str | None = None,
                 private_key_passphrase: str | None = None,
                 remote_dir: str = ".", filename_field: str | None = None,
                 content_field: str | None = None, timeout: int = 10):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.private_key_path = private_key_path
        self.private_key_passphrase = private_key_passphrase
        self.remote_dir = remote_dir
        self.filename_field = filename_field
        self.content_field = content_field
        self.timeout = timeout

    def _connect(self):
        """Lazy-import paramiko so the node definition is importable even
        when paramiko isn't installed (e.g. on systems that only use other
        destination types)."""
        import paramiko

        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        if self.private_key_path:
            key = None
            if self.private_key_path.endswith(".ppk"):
                key = paramiko.RSAKey.from_private_key_file(
                    self.private_key_path, password=self.private_key_passphrase
                )
            else:
                for key_cls in (paramiko.RSAKey, paramiko.ECDSAKey, paramiko.Ed25519Key):
                    try:
                        key = key_cls.from_private_key_file(
                            self.private_key_path, password=self.private_key_passphrase
                        )
                        break
                    except Exception:
                        continue
            if key is None:
                raise ValueError(f"Could not load private key from {self.private_key_path}")
            client.connect(
                self.host, port=self.port, username=self.username,
                pkey=key, timeout=self.timeout,
            )
        else:
            client.connect(
                self.host, port=self.port, username=self.username,
                password=self.password, timeout=self.timeout,
            )
        return client

    def send(self, payload):
        client = self._connect()
        try:
            sftp = client.open_sftp()

            filename, content = self._prepare_payload(payload)

            remote_path = os.path.join(self.remote_dir, filename)
            with sftp.open(remote_path, "wb") as f:
                if isinstance(content, str):
                    f.write(content.encode("utf-8"))
                else:
                    f.write(content)

            sftp.close()
        finally:
            client.close()

        return remote_path

    def _prepare_payload(self, payload):
        """Returns (filename, content) where content is str or bytes."""
        filename = None
        content = None

        if isinstance(payload, dict):
            # Explicit filename/content fields take priority
            if self.filename_field and self.filename_field in payload:
                filename = str(payload[self.filename_field])
            elif "filename" in payload:
                filename = str(payload["filename"])

            if self.content_field and self.content_field in payload:
                content = payload[self.content_field]
            elif "content" in payload:
                content = payload["content"]

            if filename is None:
                filename = f"message_{payload.get('trace_id', 'unknown')}.json"
            if content is None:
                content = json.dumps(payload, default=str)
        elif isinstance(payload, str):
            filename = f"message_{abs(hash(payload))}.txt"
            content = payload
        elif isinstance(payload, bytes):
            filename = f"message_{abs(hash(payload))}.bin"
            content = payload
        else:
            filename = f"message_{abs(hash(str(payload)))}.json"
            content = json.dumps(payload, default=str)

        return filename, content