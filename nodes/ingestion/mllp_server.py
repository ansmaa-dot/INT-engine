import socket
import threading
import time

from core.message import Envelope
from nodes.base import IngestionNode

VT = b"\x0b"
FS = b"\x1c"
CR = b"\x0d"


class MLLPServer(IngestionNode):
    """Case 1 inbound: a raw TCP listener speaking MLLP framing. This is the
    trickiest ingestion transport — most of what's below exists to avoid the
    two classic MLLP bugs:

    1. ACKing before the message is safely persisted. If the process dies
       between ACK and enqueue, the sending system believes delivery
       succeeded and the message is gone forever. So: enqueue first,
       ACK second, always.
    2. One slow/stuck connection blocking the whole listener. Each
       connection gets handled on its own thread with a read timeout, so a
       dead socket gets reaped instead of holding a thread hostage forever.
    """

    def __init__(self, host: str, port: int, channel_id: str, queue,
                 max_connections: int = 20, idle_timeout_s: int = 300,
                 max_queue_depth: int | None = None,
                 idempotency_from_msh10: bool = False):
        self.host = host
        self.port = port
        self.channel_id = channel_id
        self.queue = queue
        self.max_queue_depth = max_queue_depth
        self.idempotency_from_msh10 = idempotency_from_msh10

        self._sem = threading.Semaphore(max_connections)
        self.idle_timeout_s = idle_timeout_s
        self._server_socket: socket.socket | None = None
        self._stop_event = threading.Event()
        self._accept_thread: threading.Thread | None = None

    def start(self, on_message_callback=None) -> None:
        self._stop_event.clear()
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server_socket.bind((self.host, self.port))
        self._server_socket.listen(5)
        # short timeout on the accept loop so stop() can interrupt cleanly
        self._server_socket.settimeout(1.0)

        self._accept_thread = threading.Thread(target=self._accept_loop, daemon=True,
                                                 name=f"mllp-server-{self.channel_id}")
        self._accept_thread.start()
        print(f"[MLLPServer:{self.channel_id}] listening on {self.host}:{self.port}", flush=True)

    def stop(self) -> None:
        self._stop_event.set()
        if self._server_socket:
            try:
                self._server_socket.close()
            except OSError:
                pass

    def _accept_loop(self):
        while not self._stop_event.is_set():
            try:
                client_sock, addr = self._server_socket.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            threading.Thread(target=self._handle_client, args=(client_sock, addr),
                              daemon=True).start()

    def _handle_client(self, client_sock: socket.socket, addr):
        acquired = self._sem.acquire(blocking=False)
        if not acquired:
            # already at max_connections — refuse rather than let this
            # connection queue up behind others indefinitely
            client_sock.close()
            return

        client_sock.settimeout(self.idle_timeout_s)
        buffer = b""
        try:
            while not self._stop_event.is_set():
                try:
                    chunk = client_sock.recv(4096)
                except (socket.timeout, ConnectionResetError):
                    break
                if not chunk:
                    break
                buffer += chunk

                while VT in buffer and (FS + CR) in buffer:
                    start = buffer.index(VT)
                    end = buffer.index(FS + CR, start)
                    raw_hl7 = buffer[start + 1:end].decode("utf-8", errors="replace")
                    buffer = buffer[end + 2:]
                    ack = self._process_message(raw_hl7)
                    client_sock.sendall(VT + ack + FS + CR)
        finally:
            self._sem.release()
            client_sock.close()

    def _process_message(self, raw_hl7: str) -> bytes:
        control_id, ack_code = self._parse_msh(raw_hl7)

        if self.max_queue_depth is not None:
            depth = self.queue.queue_depth(self.channel_id)
            if depth >= self.max_queue_depth:
                # backpressure: NACK (AE) instead of enqueueing. The sending
                # system's own retry/interface-engine logic will resend
                # later — this is safer than silently dropping or blocking
                # the socket while the queue drains.
                return self._build_ack(control_id, "AE")

        env = Envelope(channel_id=self.channel_id, raw_payload={"hl7": raw_hl7})
        if self.idempotency_from_msh10 and control_id:
            env.idempotency_key = control_id

        accepted = self.queue.enqueue(env)  # persisted BEFORE the ACK goes out
        if not accepted:
            # duplicate message control ID — already processed, ACK success
            # anyway so the sender doesn't spin retrying a message we've
            # already accepted once
            return self._build_ack(control_id, "AA")

        return self._build_ack(control_id, "AA")

    def _parse_msh(self, raw_hl7: str):
        """Extracts field separator, MSH-10 (message control ID) for the ACK,
        best-effort — malformed input still gets an ACK, just without an
        echoed control ID."""
        try:
            segments = raw_hl7.split("\r")
            msh = next((s for s in segments if s.startswith("MSH")), None)
            if not msh:
                return None, None
            field_sep = msh[3]
            fields = msh.split(field_sep)
            control_id = fields[9] if len(fields) > 9 else None
            return control_id, field_sep
        except Exception:
            return None, None

    def _build_ack(self, control_id: str | None, ack_code: str) -> bytes:
        control_id = control_id or ""
        timestamp = time.strftime("%Y%m%d%H%M%S")
        msh = f"MSH|^~\\&|INTENGINE|INTENGINE|||{timestamp}||ACK|{control_id}-ACK|P|2.3\r"
        msa = f"MSA|{ack_code}|{control_id}\r"
        return (msh + msa).encode("utf-8")
