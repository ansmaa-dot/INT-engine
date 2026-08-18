import json

import pytest

from core.message import Envelope
from core.queue import PersistentQueue
from engine.config_loader import ChannelConfigRegistry, ConfigValidationError, DESTINATIONS, TRANSPORTS
from engine.runner import ChannelRunner
from nodes.transform.field_mapper import FieldMapper


def _registry(tmp_path) -> ChannelConfigRegistry:
    return ChannelConfigRegistry(str(tmp_path / "cfg.db"))


def _valid(channel_id="c1", **overrides):
    d = {
        "channel_id": channel_id, "name": "C1", "enabled": True, "status": "running",
        "concurrency": 1,
        "inbound_transport": "http_webhook", "inbound_transport_config": {},
        "inbound_codec": "json", "outbound_codec": "json",
        "destination": "http", "destination_config": {"endpoint_url": "http://x/api"},
        "mapping_id": None, "mapping_version": None,
        "enrichment_id": None, "enrichment_version": None,
        "retry_policy_id": "default", "semantics": None,
    }
    d.update(overrides)
    return d


class RecordingDestination:
    def __init__(self, fail=False):
        self.fail = fail
        self.sent = []

    def send(self, message):
        if self.fail:
            raise RuntimeError("destination unreachable")
        self.sent.append(message.content)


def test_seed_defaults(tmp_path):
    r = _registry(tmp_path)
    assert "default" in [p["retry_policy_id"] for p in r.list_retry_policies()]
    assert "his_to_lis" in r.load_all_configs()


def test_valid_channel_validates_clean(tmp_path):
    r = _registry(tmp_path)
    assert r.validate_channel_definition(_valid()) == []


def test_unknown_codec_rejected(tmp_path):
    r = _registry(tmp_path)
    assert any("inbound codec" in e for e in r.validate_channel_definition(_valid(inbound_codec="does-not-exist")))
    assert any("outbound codec" in e for e in r.validate_channel_definition(_valid(outbound_codec="nope")))


def test_unknown_transport_rejected(tmp_path):
    r = _registry(tmp_path)
    assert any("transport" in e for e in r.validate_channel_definition(_valid(inbound_transport="carrier_pigeon")))


def test_unknown_destination_rejected(tmp_path):
    r = _registry(tmp_path)
    assert any("destination" in e for e in r.validate_channel_definition(_valid(destination="pigeon")))


def test_missing_mapping_reference_rejected(tmp_path):
    r = _registry(tmp_path)
    assert any("mapping" in e for e in r.validate_channel_definition(_valid(mapping_id="ghost", mapping_version=1)))


def test_mapping_version_required(tmp_path):
    r = _registry(tmp_path)
    assert any("mapping_version" in e for e in r.validate_channel_definition(_valid(mapping_id="identity")))


def test_missing_retry_policy_rejected(tmp_path):
    r = _registry(tmp_path)
    assert any("retry" in e for e in r.validate_channel_definition(_valid(retry_policy_id="")))
    assert any("retry" in e for e in r.validate_channel_definition(_valid(retry_policy_id="ghost")))


def test_missing_transport_param_rejected(tmp_path):
    r = _registry(tmp_path)
    d = _valid(inbound_transport="mllp", inbound_transport_config={"host": "x"})  # no port
    assert any("port" in e for e in r.validate_channel_definition(d))


def test_invalid_save_raises_and_does_not_persist(tmp_path):
    r = _registry(tmp_path)
    with pytest.raises(ConfigValidationError):
        r.save_channel_definition(_valid(inbound_codec="nope"))
    assert "c1" not in r.load_all_configs()
def test_channel_saved_and_loaded_with_resolved_refs(tmp_path):
    r = _registry(tmp_path)
    v = r.save_mapping("m1", [{"source": "patient.name", "target": "patient.name"}])
    r.save_channel_definition(_valid(mapping_id="m1", mapping_version=v))
    cfg = r.load_all_configs()["c1"]
    assert cfg["mapping_rules"] == [{"source": "patient.name", "target": "patient.name"}]
    assert cfg["retry_policy"] == {"max_retries": 3, "base_backoff_seconds": 2}
    assert cfg["inbound_codec"] == "json"


def test_shared_mapping_across_channels(tmp_path):
    r = _registry(tmp_path)
    rules = [{"source": "patient.name", "target": "patient.name", "fn": "Uppercase"}]
    v = r.save_mapping("shared_map", rules)
    r.save_channel_definition(_valid("a1", mapping_id="shared_map", mapping_version=v))
    r.save_channel_definition(_valid("a2", mapping_id="shared_map", mapping_version=v))
    cfg = r.load_all_configs()
    assert cfg["a1"]["mapping_rules"] == rules
    assert cfg["a2"]["mapping_rules"] == rules


def test_mapping_versioning_isolates_consumers(tmp_path):
    r = _registry(tmp_path)
    v1 = r.save_mapping("m2", [{"source": "a", "target": "a"}])
    r.save_channel_definition(_valid("b1", mapping_id="m2", mapping_version=v1))

    v2 = r.save_mapping("m2", [{"source": "changed", "target": "changed"}])
    assert v2 > v1
    cfg = r.load_all_configs()
    assert cfg["b1"]["mapping_rules"] == [{"source": "a", "target": "a"}]  # pinned consumer unaffected

    r.save_channel_definition(_valid("b2", mapping_id="m2", mapping_version=v2))
    assert r.load_all_configs()["b2"]["mapping_rules"] == [{"source": "changed", "target": "changed"}]


def test_shared_enrichment_referenced_by_multiple_channels(tmp_path):
    r = _registry(tmp_path)
    v = r.save_enrichment(
        "pt_lookup", source_key_field="patient.identifiers.0.value",
        lookup_db_path=str(tmp_path / "ref.db"), target_table="patients",
        target_key_col="mrn", fields=["name"], lookup_name="pt",
    )
    r.save_channel_definition(_valid("x1", enrichment_id="pt_lookup", enrichment_version=v))
    r.save_channel_definition(_valid("x2", enrichment_id="pt_lookup", enrichment_version=v))
    cfg = r.load_all_configs()
    assert cfg["x1"]["enrichment"]["source_key_field"] == "patient.identifiers.0.value"
    assert cfg["x2"]["enrichment"]["lookup_name"] == "pt"


def test_build_runner_executes_declarative_channel(tmp_path):
    r = _registry(tmp_path)
    q = PersistentQueue(str(tmp_path / "q.db"))
    rec = RecordingDestination()

    v = r.save_mapping("rename", [{"source": "patient.name", "target": "patient.name", "fn": "Uppercase"}])
    r.save_channel_definition(_valid("e2e", mapping_id="rename", mapping_version=v))

    runner = r.build_runner("e2e", q, destination=rec)
    assert isinstance(runner, ChannelRunner)

    env = Envelope(channel_id="e2e", raw='{"patient": {"name": "ada"}}', inbound_codec="json")
    q.enqueue(env)
    assert runner.process_one() is True
    sent = json.loads(rec.sent[0])
    assert sent["patient"]["name"] == "ADA"
    assert q.dequeue_available("e2e") is None  # delivered


def test_invalid_config_fails_at_build_before_runtime(tmp_path):
    r = _registry(tmp_path)
    q = PersistentQueue(str(tmp_path / "q2.db"))
    # inject a channel row with an unknown codec (simulating a corrupt/stale config)
    with r._get_conn() as conn:
        conn.execute(
            """INSERT INTO channels (channel_id, name, inbound_transport, inbound_codec,
                outbound_codec, destination, destination_config, retry_policy_id)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            ("badch", "Bad", "http_webhook", "nope", "json", "http",
             json.dumps({"endpoint_url": "http://x"}), "default"),
        )
        conn.commit()
    with pytest.raises(ConfigValidationError):
        r.build_runner("badch", q, destination=RecordingDestination())


def test_registry_sets():
    assert isinstance(TRANSPORTS, set) and isinstance(DESTINATIONS, set)
    assert "mllp" in TRANSPORTS and "http_webhook" in TRANSPORTS
    assert {"http", "mllp", "sftp"} <= DESTINATIONS


def test_field_mapper_canonical_paths():
    canonical_dict = {"patient": {"name": "ada", "identifiers": [{"value": "42"}]}}
    mapper = FieldMapper([
        {"source": "patient.name", "target": "patient.name", "fn": "Uppercase"},
        {"source": "patient.identifiers.0.value", "target": "extensions.mrn"},
    ])
    out = mapper.transform(canonical_dict)
    assert out["patient"]["name"] == "ADA"
    assert out["extensions"]["mrn"] == "42"
    assert out["patient"]["identifiers"][0]["value"] == "42"  # untouched deep field preserved

def test_enrichment_on_canonical_via_runner(tmp_path):
    import sqlite3
    ref_db = str(tmp_path / "ref.db")
    with sqlite3.connect(ref_db) as c:
        c.execute("CREATE TABLE patients (mrn TEXT PRIMARY KEY, name TEXT)")
        c.execute("INSERT INTO patients VALUES ('42', 'Grace Hopper')")

    r = _registry(tmp_path)
    q = PersistentQueue(str(tmp_path / "q3.db"))
    rec = RecordingDestination()

    ev = r.save_enrichment(
        "pt_lookup", source_key_field="patient.identifiers.0.value",
        lookup_db_path=ref_db, target_table="patients", target_key_col="mrn",
        fields=["name"], lookup_name="pt",
    )
    mv = r.save_mapping(
        "use_lookup", [{"source": "lookups.pt.name", "target": "extensions.patient_name"}]
    )
    r.save_channel_definition(_valid(
        "enr", enrichment_id="pt_lookup", enrichment_version=ev,
        mapping_id="use_lookup", mapping_version=mv,
    ))

    runner = r.build_runner("enr", q, destination=rec)
    env = Envelope(channel_id="enr", raw='{"patient": {"identifiers": [{"value": "42"}]}}', inbound_codec="json")
    q.enqueue(env)
    assert runner.process_one() is True

    sent = json.loads(rec.sent[0])
    # enrichment resolved the canonical identifier and the mapper pulled from
    # the attached lookup value
    assert sent["extensions"]["patient_name"] == "Grace Hopper"

