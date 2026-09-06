import os

from nodes.base import DestinationNode
from core.transport import DestinationMessage


class SFTPClientNode(DestinationNode):
    """SFTP destination: uploads already-serialized content as a file to a
    remote SFTP server. Supports both password and private-key authentication.

    The filename comes from the DestinationMessage (explicit delivery
    metadata) or a generated default. There is no payload sniffing: content
    must already be str/bytes — the destination never serializes dicts.
    """

    def __init__(self, host: str, port: int = 22, username: str = "",
                 password: str | None = None, private_key_path: str | None = None,
                 private_key_passphrase: str | None = None,
                 remote_dir: str = ".", filename_field: str | None = None,
                 content_field: str | None = None, timeout: int = 10):
        # filename_field / content_field are retained for config compatibility
        # but the payload contract no longer inspects dict fields.
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

    def send(self, message: DestinationMessage):
        client = self._connect()
        try:
            sftp = client.open_sftp()

            filename, content = self._prepare_payload(message)

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

    def _prepare_payload(self, message: DestinationMessage):
        """Returns (filename, content). Filename is explicit delivery
        metadata or a generated default; content must already be str/bytes."""
        content = message.content
        if not isinstance(content, (str, bytes)):
            raise TypeError(
                "destination content must be str/bytes; destination does not serialize"
            )

        if message.filename:
            filename = message.filename
        else:
            seed = content.decode("utf-8", errors="replace") if isinstance(content, bytes) else content
            ext = "bin" if isinstance(content, bytes) else "txt"
            filename = f"message_{abs(hash(seed))}.{ext}"

        return filename, content