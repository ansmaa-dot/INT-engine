"""Pass 3 contract tests: transports produce raw content + metadata;
destinations receive already-serialized content and never serialize dicts."""
import os
import socket

import pytest

from core.queue import PersistentQueue
from core.transport import DestinationMessage, TransportMessage, to_envelope
from nodes.destination.http_client import HttpClientNode
from nodes.destination.mllp_client import END_BLOCK, START_BLOCK, MllpClientNode
from nodes.destination.sftp_client import SFTPClientNode
from nodes.ingestion.file_watcher import FileWatcher
from nodes.ingestion.http_poller import HTTPPoller
from nodes.ingestion.http_webhook import WebhookRegistry
from nodes.ingestion.mllp_server import MLLPServer


def _webapp(reg):
    from flask import Flask

    app = Flask(__name__)
    app.register_blueprint(reg.bp)
    return app


# --- inbound transport contract --------------------------------------------


def test_to_envelope_transport_message():
    msg = TransportMessage(
        raw=b'{"a":1}',
        source="c1",
        message_id="id-1",
        filename="f.json",
        content_type="application/json",
    )
    env = to_envelope("c1", msg)
    assert env.raw == '{"a":1}'
    assert env.idempotency_key == "id-1"
    assert env.inbound_codec == "json"
    assert env.channel_id == "c1"

    env2 = to_envelope("c1", TransportMessage(raw="plain"))
    assert env2.raw == "plain"
    assert env2.idempotency_key is None


def test_webhook_accepts_raw_json_without_parsing(tmp_path):
    q = PersistentQueue(tmp_path / "w.db")
    reg = WebhookRegistry(q)
    reg.register("wh")
    client = _webapp(reg).test_client()

    resp = client.post("/webhooks/wh", data=b'{"name": "Ada", "age": 30}',
                       content_type="application/json")
    assert resp.status_code == 202

    env = q.dequeue_available("wh")
    assert env is not None
    # raw body preserved byte-for-byte; the transport never parsed it
    assert env.raw == '{"name": "Ada", "age": 30}'


def test_webhook_accepts_non_json_raw_body(tmp_path):
    q = PersistentQueue(tmp_path / "w2.db")
    reg = WebhookRegistry(q)
    reg.register("wh")
    client = _webapp(reg).test_client()

    resp = client.post("/webhooks/wh", data=b"MSH|^~\\&|A|B\rPID|1", content_type="text/plain")
    assert resp.status_code == 202

    env = q.dequeue_available("wh")
    assert env is not None
    assert env.raw == "MSH|^~\\&|A|B\rPID|1"


def test_webhook_verifies_signature_over_raw_body(tmp_path):
    import hashlib
    import hmac
    q = PersistentQueue(tmp_path / "w3.db")
    reg = WebhookRegistry(q)
    secret = "shh"
    reg.register("wh", shared_secret=secret, sig_header="X-Signature")
    client = _webapp(reg).test_client()

    bad = client.post("/webhooks/wh", data=b'{"a":1}',
                      headers={"X-Signature": "wrong"}, content_type="application/json")
    assert bad.status_code == 401

    body = b'{"a":1}'
    good_sig = hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    ok = client.post("/webhooks/wh", data=body,
                     headers={"X-Signature": good_sig}, content_type="application/json")
    assert ok.status_code == 202


def test_mllp_server_enqueues_raw_without_hl7_parsing(tmp_path):
    q = PersistentQueue(tmp_path / "m.db")
    srv = MLLPServer("127.0.0.1", 0, "ch", q, idempotency_from_msh10=False)
    hl7 = ("MSH|^~\\&|S|R|||20240101000000||ORU^R01|CTRL1|P|2.3\r"
           "PID|1||456^^^MRN\rOBX|1|NM|2339-0^GLUCOSE^LN||90|mg/dL")
    ack = srv._process_message(hl7)

    assert b"MSA|AA|CTRL1" in ack  # framing/ACK behavior retained
    env = q.dequeue_available("ch")
    assert env is not None
    assert env.raw == hl7  # raw preserved; no business dict built
    assert env.idempotency_key is None


def test_mllp_server_thin_idempotency_hint(tmp_path):
    q = PersistentQueue(tmp_path / "m2.db")
    srv = MLLPServer("127.0.0.1", 0, "ch", q, idempotency_from_msh10=True)
    msg = "MSH|^~\\&|A|B|||t||ORU^R01|CTRL99|P|2.3\rOBX|1|NM|1^G||5"

    ack = srv._process_message(msg)
    assert b"MSA|AA|" in ack
    env = q.dequeue_available("ch")
    assert env is not None
    assert env.idempotency_key == "CTRL99"

    # duplicate control id -> rejected at enqueue, ACK success anyway
    srv._process_message(msg)
    assert q.queue_depth("ch") == 0  # nothing new enqueued
    assert q.dequeue_available("ch") is None


def test_file_watcher_delivers_raw_content_regardless_of_format(tmp_path):
    q = PersistentQueue(tmp_path / "fw.db")
    watch = tmp_path / "in"
    watch.mkdir()
    os.makedirs(watch / "processed", exist_ok=True)
    os.makedirs(watch / "failed", exist_ok=True)

    csv_content = "a,b\n1,2\n3,4\n"
    hl7_content = "MSH|^~\\&|A|B|2|3\rPID|1||456"
    (watch / "sample.csv").write_text(csv_content)
    (watch / "msg.hl7").write_text(hl7_content)

    fw = FileWatcher(str(watch), "c1", q, extensions=(".csv", ".hl7"))
    fw._scan_once()

    rows = []
    while True:
        e = q.dequeue_available("c1")
        if not e:
            break
        rows.append(e)
    assert len(rows) == 2

    by_content = {r.raw: r for r in rows}
    assert by_content.get(csv_content) is not None   # raw CSV file, NOT parsed to rows
    assert by_content.get(hl7_content) is not None   # raw HL7 file
    assert q.queue_depth("c1") == 0


def test_http_poller_enqueues_raw_response_body(monkeypatch, tmp_path):
    import requests
    q = PersistentQueue(tmp_path / "p.db")
    poller = HTTPPoller("http://x/api", "c1", q, auth=None, interval_s=5)

    class FakeResp:
        content = b'[{"a":1},{"a":2}]'
        headers = {"Content-Type": "application/json"}

        def raise_for_status(self):
            pass

    monkeypatch.setattr(requests, "get", lambda *a, **k: FakeResp())
    poller._poll_once()

    env = q.dequeue_available("c1")
    assert env is not None
    # raw response body enqueued as-is — no record extraction in transport
    assert env.raw == '[{"a":1},{"a":2}]'
    assert q.dequeue_available("c1") is None  # one message per tick, whole body


# --- destination contract ---------------------------------------------------


def test_http_destination_sends_serialized_content(monkeypatch):
    import requests
    captured = {}

    class FakeResp:
        status_code = 200

        def raise_for_status(self):
            pass

    def fake_request(method, url, **kwargs):
        captured["method"] = method
        captured["url"] = url
        captured["kwargs"] = kwargs
        return FakeResp()

    monkeypatch.setattr(requests, "request", fake_request)
    node = HttpClientNode("http://example/api")
    node.send(DestinationMessage(content=b'{"patient": {}}', content_type="application/json"))

    assert captured["method"] == "POST"
    assert captured["url"] == "http://example/api"
    assert captured["kwargs"]["data"] == b'{"patient": {}}'
    # it must never fall back to json= dict serialization
    assert "json" not in captured["kwargs"]


def test_http_destination_supports_configurable_content_type(monkeypatch):
    import requests
    captured = {}

    class FakeResp:
        status_code = 200

        def raise_for_status(self):
            pass

    def fake_request(method, url, **kwargs):
        captured["headers"] = kwargs.get("headers", {})
        return FakeResp()

    monkeypatch.setattr(requests, "request", fake_request)
    node = HttpClientNode("http://example/api", headers={"X-Foo": "bar"})
    node.send(DestinationMessage(content="text", content_type="text/plain"))
    assert captured["headers"]["Content-Type"] == "text/plain"
    assert captured["headers"]["X-Foo"] == "bar"


def test_http_destination_rejects_non_serialized_content():
    node = HttpClientNode("http://example/api")
    with pytest.raises(TypeError):
        node.send(DestinationMessage(content={"a": 1}))


def test_mllp_destination_frames_serialized_content(monkeypatch):
    captured = {}

    class FakeSock:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def sendall(self, data):
            captured["data"] = data

        def recv(self, n):
            return b"MSH|AA|ACK"

    monkeypatch.setattr(socket, "create_connection", lambda *a, **k: FakeSock())
    node = MllpClientNode("host", 1)
    node.send(DestinationMessage(content="MSH|PID|OBX"))

    assert captured["data"] == START_BLOCK + b"MSH|PID|OBX" + END_BLOCK


def test_mllp_destination_rejects_non_serialized_content():
    node = MllpClientNode("host", 1)
    with pytest.raises(TypeError):
        node.send(DestinationMessage(content={"a": 1}))


def test_sftp_uses_explicit_filename_and_content():
    node = SFTPClientNode("host")
    filename, content = node._prepare_payload(
        DestinationMessage(content="data", filename="orders/out.txt")
    )
    assert filename == "orders/out.txt"
    assert content == "data"


def test_sftp_generates_default_filename_for_content():
    node = SFTPClientNode("host")
    filename, content = node._prepare_payload(DestinationMessage(content="abc"))
    assert filename.endswith(".txt")
    assert content == "abc"


def test_sftp_rejects_non_serialized_content():
    node = SFTPClientNode("host")
    with pytest.raises(TypeError):
        node._prepare_payload(DestinationMessage(content={"a": 1}))