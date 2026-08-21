"""Unit and integration tests for BatchLookup and enrichment pipeline stage."""
import json
import sqlite3

import pytest

from core.message import Envelope
from core.queue import PersistentQueue
from engine.config_loader import ChannelConfigRegistry, ConfigValidationError
from engine.runner import ChannelRunner
from nodes.base import EnrichmentNode
from nodes.enrichment.batch_lookup import BatchLookup
from nodes.enrichment.db_adapter import (
    SqliteAdapter,
    DatabaseAdapter,
    build_adapter,
    is_valid_identifier,
)
from nodes.transform.field_mapper import FieldMapper


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _ref_db(tmp_path, rows=None):
    """Create a SQLite reference DB with a patients table."""
    path = str(tmp_path / "ref.db")
    with sqlite3.connect(path) as c:
        c.execute("CREATE TABLE patients (mrn TEXT PRIMARY KEY, name TEXT, dob TEXT)")
        c.execute("CREATE TABLE doctors (doctor_id TEXT PRIMARY KEY, full_name TEXT)")
        if rows:
            for r in rows:
                c.execute(
                    "INSERT OR REPLACE INTO patients (mrn, name, dob) VALUES (?, ?, ?)",
                    (r["mrn"], r["name"], r.get("dob")),
                )
            c.commit()
    return path


def _env(raw, canonical=None):
    """Shorthand: single Envelope with JSON canonical."""
    env = Envelope(channel_id="t1", raw=raw, inbound_codec="json")
    if canonical:
        import core.model as m
        env.canonical = m.CanonicalMessage.model_validate(canonical)
    return env


class FakeAdapter(DatabaseAdapter):
    def __init__(self, rows=None):
        self.rows = rows or []

    def execute(self, query, params):
        return self.rows

    def validate(self):
        pass

    def table_exists(self, table):
        return True

    def column_names(self, table):
        return ["mrn", "name"]


class RecordingDest:
    def __init__(self):
        self.sent = []

    def send(self, message):
        self.sent.append(message.content)


def _reg(tmp_path):
    return ChannelConfigRegistry(str(tmp_path / "cfg.db"))

# ---------------------------------------------------------------------------
# is_valid_identifier
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("name,expected", [
    ("patients", True),
    ("mrn", True),
    ("patient_name", True),
    ("_private", True),
    ("table123", True),
    ("", False),
    ("123start", False),
    ("has-dash", False),
    ("has space", False),
    ("drop;table", False),
])
def test_is_valid_identifier(name, expected):
    assert is_valid_identifier(name) is expected


# ---------------------------------------------------------------------------
# BatchLookup – constructor validation
# ---------------------------------------------------------------------------


def test_batch_lookup_rejects_invalid_table_name():
    with pytest.raises(Exception) as exc:
        BatchLookup(target_table="patients; DROP TABLE queue")
    assert "not a valid SQL identifier" in str(exc.value)


def test_batch_lookup_rejects_invalid_key_col():
    with pytest.raises(Exception) as exc:
        BatchLookup(target_key_col="mrn--bad")
    assert "not a valid SQL identifier" in str(exc.value)


def test_batch_lookup_rejects_invalid_field_name():
    with pytest.raises(Exception) as exc:
        BatchLookup(fields=["name", "bad field!"])
    assert "not a valid SQL identifier" in str(exc.value)


def test_batch_lookup_accepts_valid_identifiers():
    bl = BatchLookup(
        db_path="/tmp/fake.db",
        source_key_field="patient.identifiers.0.value",
        target_table="patients",
        target_key_col="mrn",
        fields=["name", "dob"],
        lookup_name="pt",
    )
    assert bl.target_table == "patients"
    assert bl.fields == ["name", "dob"]


# ---------------------------------------------------------------------------
# BatchLookup – enrich_batch (SQLite)
# ---------------------------------------------------------------------------


def test_enrich_batch_resolves_single_key(tmp_path):
    db = _ref_db(tmp_path, [{"mrn": "42", "name": "Grace Hopper"}])
    bl = BatchLookup(
        db_path=db,
        source_key_field="patient.identifiers.0.value",
        target_table="patients",
        target_key_col="mrn",
        fields=["name"],
        lookup_name="pt",
    )

    canonical = {
        "patient": {"identifiers": [{"value": "42", "type": "MRN"}]},
        "metadata": {"format": "json"},
    }
    env = _env("{}", canonical)

    bl.enrich_batch([env])
    assert env.lookups["pt"] == {"mrn": "42", "name": "Grace Hopper"}


def test_enrich_batch_batches_multiple_keys(tmp_path):
    db = _ref_db(tmp_path, [
        {"mrn": "42", "name": "Grace"},
        {"mrn": "99", "name": "Alan"},
    ])
    bl = BatchLookup(
        db_path=db,
        source_key_field="patient.identifiers.0.value",
        target_table="patients",
        target_key_col="mrn",
        fields=["name"],
        lookup_name="pt",
    )

    c1 = {"patient": {"identifiers": [{"value": "42"}]}, "metadata": {"format": "json"}}
    c2 = {"patient": {"identifiers": [{"value": "99"}]}, "metadata": {"format": "json"}}
    e1 = _env("{}", c1)
    e2 = _env("{}", c2)

    bl.enrich_batch([e1, e2])
    assert e1.lookups["pt"]["name"] == "Grace"
def test_enrich_batch_missing_key_is_none(tmp_path):
    db = _ref_db(tmp_path, [{"mrn": "42", "name": "Grace"}])
    bl = BatchLookup(
        db_path=db,
        source_key_field="patient.identifiers.0.value",
        target_table="patients",
        target_key_col="mrn",
        fields=["name"],
        lookup_name="pt",
    )

    env = _env("{}", {"patient": {"identifiers": [{"value": "999"}]}, "metadata": {"format": "json"}})
    bl.enrich_batch([env])
    assert env.lookups["pt"] is None


def test_enrich_batch_empty_envelopes_gets_none_lookup(tmp_path):
    db = _ref_db(tmp_path)
    bl = BatchLookup(
        db_path=db,
        source_key_field="patient.identifiers.0.value",
        target_table="patients",
        target_key_col="mrn",
        fields=["name"],
        lookup_name="pt",
    )
    result = bl.enrich_batch([])
    assert result == []


def test_enrich_batch_all_none_keys_gets_none(tmp_path):
    db = _ref_db(tmp_path)
    bl = BatchLookup(
        db_path=db,
        source_key_field="patient.identifiers.0.value",
        target_table="patients",
        target_key_col="mrn",
        fields=["name"],
        lookup_name="pt",
    )
    e1 = Envelope(channel_id="t1", raw="{}", inbound_codec="json")
    e2 = Envelope(channel_id="t1", raw="{}", inbound_codec="json")
    bl.enrich_batch([e1, e2])
    assert e1.lookups["pt"] is None
    assert e2.lookups["pt"] is None


def test_enrich_batch_unhashable_key_returns_none(tmp_path):
    db = _ref_db(tmp_path)
    bl = BatchLookup(
        db_path=db,
        source_key_field="patient.identifiers",  # resolves to a list
        target_table="patients",
        target_key_col="mrn",
        fields=["name"],
        lookup_name="pt",
    )
    env = _env("{}", {"patient": {"identifiers": [{"value": "42"}]}, "metadata": {"format": "json"}})
    bl.enrich_batch([env])
    assert env.lookups["pt"] is None


def test_enrich_single_convenience(tmp_path):
    db = _ref_db(tmp_path, [{"mrn": "42", "name": "Ada"}])
    bl = BatchLookup(
        db_path=db,
        source_key_field="patient.identifiers.0.value",
        target_table="patients",
        target_key_col="mrn",
        fields=["name"],
        lookup_name="pt",
    )
    env = _env("{}", {"patient": {"identifiers": [{"value": "42"}]}, "metadata": {"format": "json"}})
    result = bl.enrich(env)
    assert result.lookups["pt"]["name"] == "Ada"


def test_batch_lookup_with_injected_adapter():
    adapter = FakeAdapter([{"mrn": "42", "name": "Dr. Test"}])
    bl = BatchLookup(
        source_key_field="patient.identifiers.0.value",
        target_table="patients",
        target_key_col="mrn",
        fields=["name"],
        lookup_name="pt",
        db_adapter=adapter,
    )
    env = _env("{}", {"patient": {"identifiers": [{"value": "42"}]}, "metadata": {"format": "json"}})
    bl.enrich_batch([env])
    assert env.lookups["pt"]["name"] == "Dr. Test"


def test_build_adapter_sqlite(tmp_path):
    db = _ref_db(tmp_path)
    a = build_adapter("sqlite", db_path=db)
    assert isinstance(a, SqliteAdapter)
    a.validate()


def test_build_adapter_unknown_type_raises():
    with pytest.raises(ValueError, match="unsupported db_type"):
        build_adapter("oracle")

# ---------------------------------------------------------------------------
# Pipeline integration: enrichment -> transform via runner
# ---------------------------------------------------------------------------


def test_runner_enrichment_and_mapper_integration(tmp_path):
    db = _ref_db(tmp_path, [{"mrn": "42", "name": "Grace Hopper"}])

    q = PersistentQueue(str(tmp_path / "q.db"))
    rec = RecordingDest()

    bl = BatchLookup(
        db_path=db,
        source_key_field="patient.identifiers.0.value",
        target_table="patients",
        target_key_col="mrn",
        fields=["name"],
        lookup_name="pt",
    )
    mapper = FieldMapper([
        {"source": "lookups.pt.name", "target": "extensions.patient_name"}
    ])

    runner = ChannelRunner(
        "c1", q,
        mapper=mapper,
        enricher=bl,
        destination=rec,
        inbound_codec="json",
        outbound_codec="json",
    )

    env = Envelope(
        channel_id="c1",
        raw='{"patient": {"identifiers": [{"value": "42"}]}}',
        inbound_codec="json",
    )
    q.enqueue(env)

    assert runner.process_one() is True
    sent = json.loads(rec.sent[0])
    assert sent["extensions"]["patient_name"] == "Grace Hopper"


def test_runner_enrichment_failure_is_permanent_dlq(tmp_path):
    q = PersistentQueue(str(tmp_path / "q.db"))

    class BrokenEnricher(EnrichmentNode):
        def enrich_batch(self, envelopes):
            raise RuntimeError("lookup db is down")

    runner = ChannelRunner(
        "c1", q,
        enricher=BrokenEnricher(),
        inbound_codec="json",
        outbound_codec="json",
    )

    env = Envelope(channel_id="c1", raw='{"patient": {}}', inbound_codec="json")
    q.enqueue(env)

    assert runner.process_one() is True  # "did work" — not "did succeed"
    with q._get_conn() as conn:
        row = conn.execute(
            "SELECT state FROM queue WHERE trace_id = ?", (env.trace_id,)
        ).fetchone()
    assert row is not None
    assert row["state"] == "DEAD_LETTER"

# ---------------------------------------------------------------------------
# Config registry: enrichment save/load
# ---------------------------------------------------------------------------


def test_save_enrichment_rejects_invalid_identifier(tmp_path):
    r = _reg(tmp_path)
    with pytest.raises(ConfigValidationError, match="not a valid SQL identifier"):
        r.save_enrichment(
            "bad", source_key_field="x",
            lookup_db_path=str(tmp_path / "r.db"),
            target_table="bad;table",
            target_key_col="mrn",
            fields=["name"],
            lookup_name="x",
        )


def test_save_enrichment_rejects_unsupported_db_type(tmp_path):
    r = _reg(tmp_path)
    with pytest.raises(ConfigValidationError, match="unsupported db_type"):
        r.save_enrichment(
            "bad", source_key_field="x",
            lookup_db_path=str(tmp_path / "r.db"),
            target_table="patients",
            target_key_col="mrn",
            fields=["name"],
            lookup_name="x",
            db_type="oracle",
        )


def test_save_enrichment_requires_connection_string_for_external(tmp_path):
    r = _reg(tmp_path)
    with pytest.raises(ConfigValidationError, match="connection_string is required"):
        r.save_enrichment(
            "bad", source_key_field="x",
            lookup_db_path=str(tmp_path / "r.db"),
            target_table="patients",
            target_key_col="mrn",
            fields=["name"],
            lookup_name="x",
            db_type="postgresql",
        )


def test_save_and_build_enrichment_with_explicit_db_type(tmp_path):
    r = _reg(tmp_path)
    ref_db = _ref_db(tmp_path, [{"mrn": "1", "name": "Test"}])
    v = r.save_enrichment(
        "ext_lookup", source_key_field="patient.identifiers.0.value",
        lookup_db_path=ref_db, target_table="patients", target_key_col="mrn",
        fields=["name"], lookup_name="pt",
        db_type="sqlite",
    )
    r.save_channel_definition({
        "channel_id": "ext_ch", "name": "Ext", "enabled": True, "status": "running",
        "concurrency": 1,
        "inbound_transport": "http_webhook", "inbound_transport_config": {},
        "inbound_codec": "json", "outbound_codec": "json",
        "destination": "http", "destination_config": {"endpoint_url": "http://x/api"},
        "mapping_id": None, "mapping_version": None,
        "enrichment_id": "ext_lookup", "enrichment_version": v,
        "retry_policy_id": "default", "semantics": None,
    })

    q = PersistentQueue(str(tmp_path / "q.db"))
    runner = r.build_runner("ext_ch", q, destination=RecordingDest())
    assert runner.enricher is not None
    assert runner.enricher.lookup_name == "pt"

def test_build_runner_validates_missing_table_at_build_time(tmp_path):
    r = _reg(tmp_path)
    ref_db = _ref_db(tmp_path, [{"mrn": "1", "name": "X"}])

    with r._get_conn() as conn:
        conn.execute(
            """INSERT INTO enrichments
               (enrichment_id, version, source_key_field, lookup_db_path,
                target_table, target_key_col, fields, lookup_name,
                db_type, connection_string, description, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            ("bad_tbl", 1, "patient.identifiers.0.value", ref_db,
             "nonexistent_table", "mrn", json.dumps(["name"]), "pt",
             "sqlite", None, "", "2024-01-01", "2024-01-01"),
        )
        conn.commit()

    r.save_channel_definition({
        "channel_id": "bad_tbl_ch", "name": "Bad", "enabled": True, "status": "running",
        "concurrency": 1,
        "inbound_transport": "http_webhook", "inbound_transport_config": {},
        "inbound_codec": "json", "outbound_codec": "json",
        "destination": "http", "destination_config": {"endpoint_url": "http://x/api"},
        "mapping_id": None, "mapping_version": None,
        "enrichment_id": "bad_tbl", "enrichment_version": 1,
        "retry_policy_id": "default", "semantics": None,
    })

    q = PersistentQueue(str(tmp_path / "q.db"))
    with pytest.raises(ConfigValidationError, match="table .* not found"):
        r.build_runner("bad_tbl_ch", q, destination=RecordingDest())


def test_build_runner_validates_missing_column_at_build_time(tmp_path):
    r = _reg(tmp_path)
    ref_db = _ref_db(tmp_path, [{"mrn": "1", "name": "X"}])

    with r._get_conn() as conn:
        conn.execute(
            """INSERT INTO enrichments
               (enrichment_id, version, source_key_field, lookup_db_path,
                target_table, target_key_col, fields, lookup_name,
                db_type, connection_string, description, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            ("bad_col", 1, "patient.identifiers.0.value", ref_db,
             "patients", "mrn", json.dumps(["nonexistent_column"]), "pt",
             "sqlite", None, "", "2024-01-01", "2024-01-01"),
        )
        conn.commit()

    r.save_channel_definition({
        "channel_id": "bad_col_ch", "name": "Bad", "enabled": True, "status": "running",
        "concurrency": 1,
        "inbound_transport": "http_webhook", "inbound_transport_config": {},
        "inbound_codec": "json", "outbound_codec": "json",
        "destination": "http", "destination_config": {"endpoint_url": "http://x/api"},
        "mapping_id": None, "mapping_version": None,
        "enrichment_id": "bad_col", "enrichment_version": 1,
        "retry_policy_id": "default", "semantics": None,
    })

    q = PersistentQueue(str(tmp_path / "q.db"))
    with pytest.raises(ConfigValidationError, match="field .* not found"):
        r.build_runner("bad_col_ch", q, destination=RecordingDest())
