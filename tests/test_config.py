import json
import sqlite3

import pytest

from core.message import Envelope
from core.queue import PersistentQueue
from engine.config_loader import (
    ChannelConfigRegistry,
    ConfigValidationError,
    DESTINATIONS,
    TRANSPORTS,
)
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
        "pipeline": [],
        "retry_policy_id": "default",
    }
    d.update(overrides)
    return d


def _transform_step(step_id="t1", rules=None, description=None):
    entry = {
        "step_id": step_id, "type": "transform",
        "config": {"rules": rules or [{"source": "patient.name", "target": "patient.name"}]},
    }
    if description is not None:
        entry["description"] = description
    return entry


def _enrich_step(step_id="e1", lookup_db_path="/tmp/ref.db", **overrides):
    cfg = {
        "source_key_field": "patient.identifiers.0.value",
        "lookup_db_path": lookup_db_path,
        "target_table": "patients",
        "target_key_col": "mrn",
        "fields": ["name"],
        "lookup_name": "pt",
    }
    cfg.update(overrides)
    return {"step_id": step_id, "type": "enrich", "config": cfg}


def _filter_step(step_id="f1", expression=None, on_fail=None):
    cfg = {"expression": expression or {"truthy": [{"var": ["patient.name"]}]}}
    if on_fail is not None:
        cfg["on_fail"] = on_fail
    return {"step_id": step_id, "type": "filter", "config": cfg}


def _assert_step(step_id="a1", expression=None, on_fail=None):
    cfg = {"expression": expression or {"not_empty": [{"var": ["patient.name"]}]}}
    if on_fail is not None:
        cfg["on_fail"] = on_fail
    return {"step_id": step_id, "type": "assert", "config": cfg}


class RecordingDestination:
    def __init__(self, fail=False):
        self.fail = fail
        self.sent = []

    def send(self, message):
        if self.fail:
            raise RuntimeError("destination unreachable")
        self.sent.append(message.content)


# ---------------------------------------------------------------------------
# schema / seeding
# ---------------------------------------------------------------------------



def test_seed_defaults(tmp_path):
    r = _registry(tmp_path)
    assert "default" in [p["retry_policy_id"] for p in r.list_retry_policies()]
    assert "his_to_lis" in r.load_all_configs()
    # shared identity mapping + demo channel with an empty pipeline
    shared = r.list_shared_steps("mapping")
    assert {"kind": "mapping", "id": "identity", "version": 1,
            "description": "identity mapping (no changes)"} in shared
    assert r.load_all_configs()["his_to_lis"]["pipeline"] == []


def test_schema_hard_reset_on_version_mismatch(tmp_path):
    db = str(tmp_path / "cfg.db")
    with sqlite3.connect(db) as c:
        c.execute("CREATE TABLE mappings (mapping_id TEXT, version INT, rules TEXT)")
        c.execute("INSERT INTO mappings VALUES ('legacy', 1, '[]')")
        c.commit()  # user_version stays 0 -> mismatch on open
    r = ChannelConfigRegistry(db)
    with r._get_conn() as c:
        tables = {row[0] for row in c.execute(
            "SELECT name FROM sqlite_master WHERE type='table'")}
        assert "mappings" not in tables
        assert "shared_steps" in tables
        assert "pipeline_steps" in tables
        assert c.execute("PRAGMA user_version").fetchone()[0] == 1
    # reseeded from scratch
    assert "his_to_lis" in r.load_all_configs()


# ---------------------------------------------------------------------------
# basic channel validation (unchanged contract)
# ---------------------------------------------------------------------------


def test_valid_channel_validates_clean(tmp_path):
    r = _registry(tmp_path)
    assert r.validate_channel_definition(_valid()) == []
    assert r.validate_channel_definition(_valid(pipeline=[
        _transform_step(), _filter_step(), _assert_step()])) == []


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


def test_pipeline_must_be_array(tmp_path):
    r = _registry(tmp_path)
    assert any("pipeline must be a JSON array" in e
               for e in r.validate_channel_definition(_valid(pipeline={"type": "transform"})))


# ---------------------------------------------------------------------------
# pipeline step validation (fail-fast)
# ---------------------------------------------------------------------------


def test_unknown_step_type_rejected(tmp_path):
    r = _registry(tmp_path)
    errors = r.validate_channel_definition(_valid(pipeline=[
        {"step_id": "s", "type": "explode", "config": {}}]))
    assert any("unknown step type" in e for e in errors)


def test_step_requires_config_or_shared(tmp_path):
    r = _registry(tmp_path)
    errors = r.validate_channel_definition(_valid(pipeline=[
        {"step_id": "s", "type": "transform"}]))
    assert any("requires either inline 'config' or a 'shared' reference" in e for e in errors)


def test_step_cannot_have_both_config_and_shared(tmp_path):
    r = _registry(tmp_path)
    errors = r.validate_channel_definition(_valid(pipeline=[
        {"step_id": "s", "type": "transform", "config": {"rules": []},
         "shared": {"kind": "mapping", "id": "identity", "version": 1}}]))
    assert any("not both" in e for e in errors)


def test_inline_step_id_assigned_and_duplicates_rejected(tmp_path):
    r = _registry(tmp_path)
    r.save_channel_definition(_valid("auto", pipeline=[
        {"type": "transform", "config": {"rules": []}}]))
    cfg = r.load_all_configs()["auto"]
    assert cfg["pipeline"][0]["step_id"]  # assigned on save

    errors = r.validate_channel_definition(_valid(pipeline=[
        _transform_step("dup"), _transform_step("dup")]))
    assert any("duplicate step_id" in e for e in errors)


def test_missing_shared_reference_rejected(tmp_path):
    r = _registry(tmp_path)
    errors = r.validate_channel_definition(_valid(pipeline=[
        {"step_id": "s1", "type": "transform",
         "shared": {"kind": "mapping", "id": "ghost", "version": 1}}]))
    assert any("version 1 does not exist" in e for e in errors)


def test_shared_reference_version_required(tmp_path):
    r = _registry(tmp_path)
    errors = r.validate_channel_definition(_valid(pipeline=[
        {"step_id": "s1", "type": "transform",
         "shared": {"kind": "mapping", "id": "identity"}}]))
    assert any("positive integer version" in e for e in errors)


def test_shared_kind_must_match_step_type(tmp_path):
    r = _registry(tmp_path)
    errors = r.validate_channel_definition(_valid(pipeline=[
        {"step_id": "s1", "type": "transform",
         "shared": {"kind": "enrichment", "id": "identity", "version": 1}}]))
    assert any("does not match step type" in e for e in errors)


def test_unknown_transform_source_target_fn_rejected(tmp_path):
    r = _registry(tmp_path)
    errors = r.validate_channel_definition(_valid(pipeline=[
        _transform_step("t", [{"source": "patient.nope", "target": "patient.name",
                               "fn": "NotExist"}])]))
    assert any("unknown source field" in e for e in errors)
    assert any("unknown transform fn" in e for e in errors)

    errors = r.validate_channel_definition(_valid(pipeline=[
        _transform_step("t", [{"source": "patient.name", "target": "x.y.z"}])]))
    assert any("unknown target field" in e for e in errors)


def test_whole_identifier_object_target_rejected(tmp_path):
    r = _registry(tmp_path)
    errors = r.validate_channel_definition(_valid(pipeline=[
        _transform_step("t", [{"source": "patient.name", "target": "patient.identifiers.0"}])]))
    # whole-Identifier-object targets are not catalog leaves — rejected
    assert any("unknown target field" in e for e in errors)


def test_filter_on_fail_vocabulary(tmp_path):
    r = _registry(tmp_path)
    assert r.validate_channel_definition(_valid(pipeline=[
        _filter_step("f", on_fail="discard")])) == []
    errors = r.validate_channel_definition(_valid(pipeline=[
        _filter_step("f", on_fail="retry")]))
    assert any("filter on_fail must be one of" in e for e in errors)


def test_assert_on_fail_vocabulary(tmp_path):
    r = _registry(tmp_path)
    assert r.validate_channel_definition(_valid(pipeline=[
        _assert_step("a", on_fail="retry")])) == []
    errors = r.validate_channel_definition(_valid(pipeline=[
        _assert_step("a", on_fail="discard")]))
    assert any("assert on_fail must be one of" in e for e in errors)


def test_condition_expression_unknown_field_rejected(tmp_path):
    r = _registry(tmp_path)
    errors = r.validate_channel_definition(_valid(pipeline=[
        _filter_step("f", {"truthy": [{"var": ["patient.nope"]}]})]))
    assert any("unknown canonical field" in e for e in errors)

    # lookups.* is the runtime enrichment namespace — statically allowed
    assert r.validate_channel_definition(_valid(pipeline=[
        _assert_step("a", {"not_empty": [{"var": ["lookups.pt.name"]}]})])) == []


def test_inline_enrich_validation(tmp_path):
    r = _registry(tmp_path)
    errors = r.validate_channel_definition(_valid(pipeline=[
        _enrich_step("e", target_table="bad;table")]))
    assert any("not a valid SQL identifier" in e for e in errors)

    errors = r.validate_channel_definition(_valid(pipeline=[
        _enrich_step("e", db_type="oracle")]))
    assert any("unsupported db_type" in e for e in errors)

    errors = r.validate_channel_definition(_valid(pipeline=[
        _enrich_step("e", db_type="postgresql")]))
    assert any("connection_string is required" in e for e in errors)

    errors = r.validate_channel_definition(_valid(pipeline=[
        _enrich_step("e", fields=[])]))
    assert any("fields" in e for e in errors)


# ---------------------------------------------------------------------------
# shared step CRUD (immutable versions)
# ---------------------------------------------------------------------------


def test_save_shared_step_immutable_versions(tmp_path):
    r = _registry(tmp_path)
    v1 = r.save_shared_step("mapping", "m", {"rules": []})
    assert v1 == 1
    with pytest.raises(ConfigValidationError, match="immutable"):
        r.save_shared_step("mapping", "m", {"rules": []}, version=v1)
    v2 = r.save_shared_step("mapping", "m", {
        "rules": [{"source": "patient.name", "target": "patient.name"}]})
    assert v2 == v1 + 1
    listed = [s for s in r.list_shared_steps("mapping") if s["id"] == "m"]
    assert [s["version"] for s in listed] == [1, 2]


def test_save_shared_step_validates_config(tmp_path):
    r = _registry(tmp_path)
    with pytest.raises(ConfigValidationError, match="unknown transform fn"):
        r.save_shared_step("mapping", "bad", {
            "rules": [{"source": "patient.name", "target": "patient.name",
                       "fn": "NotExist"}]})
    with pytest.raises(ConfigValidationError, match="unknown shared step kind"):
        r.save_shared_step("sprocket", "x", {})


def test_save_shared_filter_with_discard_on_fail(tmp_path):
    r = _registry(tmp_path)
    v = r.save_shared_step("filter", "drop_empty", {
        "expression": {"missing": [{"var": ["patient.name"]}]},
        "on_fail": "discard"})
    listed = r.list_shared_steps("filter")
    assert {"kind": "filter", "id": "drop_empty", "version": v,
            "description": ""} in listed


# ---------------------------------------------------------------------------
# channel save/load with resolved pipeline
# ---------------------------------------------------------------------------


def test_channel_saved_and_loaded_with_resolved_refs(tmp_path):
    r = _registry(tmp_path)
    v = r.save_shared_step("mapping", "m1", {
        "rules": [{"source": "patient.name", "target": "patient.name"}]})
    r.save_channel_definition(_valid(pipeline=[
        {"step_id": "s1", "type": "transform",
         "shared": {"kind": "mapping", "id": "m1", "version": v}}]))
    cfg = r.load_all_configs()["c1"]
    step = cfg["pipeline"][0]
    assert step["config"]["rules"] == [{"source": "patient.name", "target": "patient.name"}]
    assert step["shared_ref"] == {"kind": "mapping", "id": "m1", "version": v}
    assert cfg["retry_policy"] == {"max_retries": 3, "base_backoff_seconds": 2}
    assert cfg["inbound_codec"] == "json"


def test_shared_transform_across_channels(tmp_path):
    r = _registry(tmp_path)
    rules = [{"source": "patient.name", "target": "patient.name", "fn": "Uppercase"}]
    v = r.save_shared_step("mapping", "shared_map", {"rules": rules})
    shared = {"kind": "mapping", "id": "shared_map", "version": v}
    r.save_channel_definition(_valid("a1", pipeline=[
        {"step_id": "s1", "type": "transform", "shared": shared}]))
    r.save_channel_definition(_valid("a2", pipeline=[
        {"step_id": "s2", "type": "transform", "shared": shared}]))
    cfg = r.load_all_configs()
    assert cfg["a1"]["pipeline"][0]["config"]["rules"] == rules
    assert cfg["a2"]["pipeline"][0]["config"]["rules"] == rules


def test_shared_step_versioning_isolates_consumers(tmp_path):
    r = _registry(tmp_path)
    v1 = r.save_shared_step("mapping", "m2", {
        "rules": [{"source": "patient.name", "target": "patient.name"}]})
    r.save_channel_definition(_valid("b1", pipeline=[
        {"step_id": "s1", "type": "transform",
         "shared": {"kind": "mapping", "id": "m2", "version": v1}}]))

    v2 = r.save_shared_step("mapping", "m2", {
        "rules": [{"source": "patient.dob", "target": "patient.dob"}]})
    assert v2 > v1
    cfg = r.load_all_configs()
    # pinned consumer unaffected by the new version
    assert cfg["b1"]["pipeline"][0]["config"]["rules"] == [
        {"source": "patient.name", "target": "patient.name"}]

    r.save_channel_definition(_valid("b2", pipeline=[
        {"step_id": "s2", "type": "transform",
         "shared": {"kind": "mapping", "id": "m2", "version": v2}}]))
    assert r.load_all_configs()["b2"]["pipeline"][0]["config"]["rules"] == [
        {"source": "patient.dob", "target": "patient.dob"}]


def test_shared_enrichment_referenced_by_multiple_channels(tmp_path):
    r = _registry(tmp_path)
    v = r.save_shared_step("enrichment", "pt_lookup", {
        "source_key_field": "patient.identifiers.0.value",
        "lookup_db_path": str(tmp_path / "ref.db"),
        "target_table": "patients", "target_key_col": "mrn",
        "fields": ["name"], "lookup_name": "pt",
    })
    shared = {"kind": "enrichment", "id": "pt_lookup", "version": v}
    r.save_channel_definition(_valid("x1", pipeline=[
        {"step_id": "s1", "type": "enrich", "shared": shared}]))
    r.save_channel_definition(_valid("x2", pipeline=[
        {"step_id": "s2", "type": "enrich", "shared": shared}]))
    cfg = r.load_all_configs()
    assert cfg["x1"]["pipeline"][0]["config"]["source_key_field"] == "patient.identifiers.0.value"
    assert cfg["x2"]["pipeline"][0]["config"]["lookup_name"] == "pt"


def test_inline_step_versions_bump_under_stable_step_id(tmp_path):
    r = _registry(tmp_path)
    r.save_channel_definition(_valid("v1", pipeline=[
        _transform_step("stable", [{"source": "patient.name", "target": "patient.name"}])]))
    r.save_channel_definition(_valid("v1", pipeline=[
        _transform_step("stable", [{"source": "patient.dob", "target": "patient.dob"}])]))
    with r._get_conn() as conn:
        rows = conn.execute(
            "SELECT version, config FROM pipeline_steps WHERE step_id = ? ORDER BY version",
            ("stable",)).fetchall()
    assert [row["version"] for row in rows] == [1, 2]
    assert json.loads(rows[0]["config"])["rules"][0]["source"] == "patient.name"
    assert json.loads(rows[1]["config"])["rules"][0]["source"] == "patient.dob"


def test_shared_step_snapshot_records_provenance(tmp_path):
    r = _registry(tmp_path)
    v = r.save_shared_step("mapping", "prov", {
        "rules": [{"source": "patient.name", "target": "patient.name"}]})
    r.save_channel_definition(_valid("p1", pipeline=[
        {"step_id": "s1", "type": "transform",
         "shared": {"kind": "mapping", "id": "prov", "version": v}}]))
    with r._get_conn() as conn:
        row = conn.execute(
            "SELECT config, provenance FROM pipeline_steps WHERE step_id = 's1'").fetchone()
    assert json.loads(row["config"])["rules"] == [
        {"source": "patient.name", "target": "patient.name"}]
    assert json.loads(row["provenance"]) == {
        "kind": "mapping", "id": "prov", "version": v}


# ---------------------------------------------------------------------------
# build_runner (declarative end-to-end)
# ---------------------------------------------------------------------------


def test_build_runner_executes_declarative_channel(tmp_path):
    r = _registry(tmp_path)
    q = PersistentQueue(str(tmp_path / "q.db"))
    rec = RecordingDestination()

    r.save_channel_definition(_valid("e2e", pipeline=[_transform_step("t1", [
        {"source": "patient.name", "target": "patient.name", "fn": "Uppercase"}])]))

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


def test_extensions_paths_accepted_as_rule_source(tmp_path):
    """Schemaless inbound data is preserved under ``extensions.*``; map rules
    must be allowed to read it even though it is not a static catalog leaf."""
    r = _registry(tmp_path)
    d = _valid("extsrc", pipeline=[
        _transform_step("t1", [
            {"source": "extensions.vendor.flag", "target": "patient.name"},
        ]),
    ])
    assert r.validate_channel_definition(d) == []

    d_enrich = _valid(
        "enrichsrc",
        pipeline=[_enrich_step("e1", source_key_field="extensions.mrn")],
    )
    assert r.validate_channel_definition(d_enrich) == []


def test_shape_hint_non_fatal_for_schemaless_transport(tmp_path):
    """transport↔codec shape mismatch is advisory: valid channel, plus a hint."""
    r = _registry(tmp_path)
    d = _valid(inbound_transport="db_poller",
               inbound_transport_config={"connection_string": "sqlite:///x.db",
                                         "query": "SELECT 1"},
               inbound_codec="hl7v2.5.1.ORU_R01")
    # Schemaless transport + structured codec is structurally VALID...
    assert r.validate_channel_definition(d) == []
    # ...but produces a non-fatal advisory hint.
    hints = r.channel_shape_hints(d)
    assert len(hints) == 1
    assert "schemaless.json" in hints[0]

    coherent = _valid(inbound_codec="schemaless.json")
    assert r.channel_shape_hints(coherent) == []


def test_enrichment_on_canonical_via_runner(tmp_path):
    import sqlite3
    ref_db = str(tmp_path / "ref.db")
    with sqlite3.connect(ref_db) as c:
        c.execute("CREATE TABLE patients (mrn TEXT PRIMARY KEY, name TEXT)")
        c.execute("INSERT INTO patients VALUES ('42', 'Grace Hopper')")

    r = _registry(tmp_path)
    q = PersistentQueue(str(tmp_path / "q3.db"))
    rec = RecordingDestination()

    r.save_channel_definition(_valid("enr", pipeline=[
        _enrich_step("e1", lookup_db_path=ref_db),
        _transform_step("t1", [
            {"source": "lookups.pt.name", "target": "extensions.patient_name"}]),
    ]))
