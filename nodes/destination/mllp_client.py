import socket
from nodes.base import DestinationNode
from core.transport import DestinationMessage

# Standard MLLP Framing Bytes
START_BLOCK = b'\x0b'
END_BLOCK = b'\x1c\x0d'


class MllpClientNode(DestinationNode):
    """MLLP destination: frames already-serialized message content and
    delivers it. It only does framing/delivery — never content parsing."""

    def __init__(self, host: str, port: int, timeout: int = 5):
        self.host = host
        self.port = port
        self.timeout = timeout

    def send(self, message: DestinationMessage):
        content = message.content
        if isinstance(content, str):
            content = content.encode("utf-8")
        elif not isinstance(content, bytes):
            raise TypeError(
                "destination content must be str/bytes; destination does not serialize"
            )

        framed_message = START_BLOCK + content + END_BLOCK

        with socket.create_connection((self.host, self.port), timeout=self.timeout) as sock:
            sock.sendall(framed_message)
            ack = sock.recv(4096)
            if not ack:
                raise ConnectionError("No MLLP ACK received from remote host")
            return ack.decode('utf-8', errors='ignore')
