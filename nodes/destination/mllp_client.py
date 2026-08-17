import socket
import json
from nodes.base import DestinationNode

# Standard MLLP Framing Bytes
START_BLOCK = b'\x0b'
END_BLOCK = b'\x1c\x0d'

class MllpClientNode(DestinationNode):
    def __init__(self, host: str, port: int, timeout: int = 5):
        self.host = host
        self.port = port
        self.timeout = timeout

    def send(self, hl7_payload):
        print(hl7_payload)
        # Convert dictionary or non-string payloads to a formatted string representation
        if isinstance(hl7_payload, dict):
            # Check if dict already contains a raw HL7 string key, otherwise serialize to JSON
            hl7_payload = hl7_payload.get("hl7") or hl7_payload.get("raw") or json.dumps(hl7_payload)
        elif not isinstance(hl7_payload, str):
            hl7_payload = str(hl7_payload)

        framed_message = START_BLOCK + hl7_payload.encode('utf-8') + END_BLOCK
        
        with socket.create_connection((self.host, self.port), timeout=self.timeout) as sock:
            sock.sendall(framed_message)
            ack = sock.recv(4096)
            if not ack:
                raise ConnectionError("No MLLP ACK received from remote host")
            return ack.decode('utf-8', errors='ignore')
