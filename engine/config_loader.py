"""Channel configuration: declarative channels with an ordered step-chain
pipeline.

A channel is orchestration ONLY:

    Inbound Transport + Inbound Codec + ordered pipeline steps
  + Outbound Codec + Destination + Retry Policy ref

The pipeline is an ordered JSON array of typed steps (enrich / transform /
filter / assert) stored on the channel row. Steps are either *inline*
(config lives in the array itself) or *shared* (a pinned
``{"kind", "id", "version"}`` reference into the generic ``shared_steps``
table). Every save appends resolved per-step snapshots to the immutable
``pipeline_steps`` history keyed by stable ``step_id`` + version.

The schema is stamped via SQLite ``PRAGMA user_version``; any mismatch
triggers a hard reset (drop + recreate + reseed). There is deliberately no
migration path from the legacy mapping/enrichment schema (plan §4 D1).

Configuration validation is explicit and fail-fast. Unknown transports,
codecs, destinations, step types, and missing shared/retry references are
rejected at save/validate time — there is never a silent fallback (e.g. no
implicit passthrough codec).
"""
import json
import re
import sqlite3
import uuid
from datetime import datetime, timezone

from core.auth_manager import AuthManager
from core.expression import validate_expression
from core.field_catalog import (
    all_catalog_paths,
    identifier_object_paths,
    validate_canonical_path,
)
from engine.runner import ChannelRunner
from engine.steps import (
    ASSERT_ON_FAIL,
    FILTER_ON_FAIL,
    SHARED_KIND_TO_STEP_TYPE,
    SHARED_KINDS,
    STEP_TYPE_TO_SHARED_KIND,
    STEP_TYPES,
    Step,
    build_step,
)
from nodes.codec import keys as codec_keys
from nodes.destination.http_client import HttpClientNode
from nodes.destination.mllp_client import MllpClientNode
from nodes.destination.sftp_client import SFTPClientNode
from nodes.enrichment.batch_lookup import BatchLookup
from nodes.enrichment.db_adapter import (
    build_adapter,
    is_valid_identifier,
)
from nodes.ingestion.db_poller import DBPoller
from nodes.ingestion.file_watcher import FileWatcher
from nodes.ingestion.http_poller import HTTPPoller
from nodes.ingestion.mllp_server import MLLPServer
from nodes.transform.field_mapper import FieldMapper
from nodes.transform.functions import REGISTRY as TRANSFORM_FN_REGISTRY

# Explicit registries of supported transports / destinations. Adding a new
# one is one entry here plus one builder branch below.
TRANSPORTS = {"http_webhook", "mllp", "http_poller", "file_watcher", "db_poller"}
DESTINATIONS = {"http", "mllp", "sftp"}

# Schema stamp (PRAGMA user_version). Bump = hard reset, not a migration.
SCHEMA_VERSION = 1

_ENRICH_DB_TYPES = ("sqlite", "postgresql", "postgres", "mysql")
_STEP_ID_RE = re.compile(r"^[A-Za-z0-9_-]{1,64}$")
_CATALOG_PATHS = all_catalog_paths()
_IDENTIFIER_OBJECT_PATHS = identifier_object_paths()


class ConfigValidationError(ValueError):
    """Raised when a channel/definition configuration is invalid."""


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _json_text(obj) -> str | None:
    if obj is None:
        return None
    if isinstance(obj, (dict, list)):
        return json.dumps(obj, default=str)
    return str(obj)


def _valid_source_path(path: str) -> bool:
    """Rule sources may be canonical paths or ``lookups.*`` (the runtime
    enrichment namespace, which cannot be statically known)."""
    return path.startswith("lookups.") or validate_canonical_path(path)


def _valid_target_path(path: str) -> bool:
    """Rule targets may be canonical paths or freeform ``extensions.*``."""
    return (
        path == "extensions"
        or path.startswith("extensions.")
        or validate_canonical_path(path)
    )


def _canonical_index_zero(path: str) -> str:
    """Canonicalize numeric path segments to ``0`` (mirrors
    ``field_catalog.validate_canonical_path``'s index handling), so
    ``patient.identifiers.2.value`` can be checked against catalog paths
    expressed with a ``0`` index."""
    parts = []
    for p in path.split("."):
        parts.append("0" if p.isdigit() or (p.startswith("-") and p[1:].isdigit()) else p)
    return ".".join(parts)


_auth_manager: AuthManager | None = None


def get_auth_manager() -> AuthManager:
    """Process-wide AuthManager singleton, loaded once from
    config/auth_profiles.json."""
    global _auth_manager
    if _auth_manager is None:
        _auth_manager = AuthManager.from_file()
    return _auth_manager


class ChannelConfigRegistry:
    def __init__(self, db_path: str = "queue.db"):
        if not db_path or not db_path.endswith(".db"):
            self.db_path = "queue.db"
        else:
            self.db_path = db_path
        self.configs = {}
        self.auth = get_auth_manager()
        self._ensure_schema()

    # --- schema ------------------------------------------------------------

    def _get_conn(self):
        conn = sqlite3.connect(self.db_path, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=30000;")
        return conn

    def _ensure_schema(self):
        with self._get_conn() as conn:
            current = conn.execute("PRAGMA user_version").fetchone()[0]
            if current != SCHEMA_VERSION:
                # Hard reset (plan §4 D1): no migration path from legacy
                # mapping/enrichment schemas — drop and reseed instead.
                self._hard_reset(conn)
                # PRAGMA cannot be parameterized; SCHEMA_VERSION is a fixed
                # module constant, not user input.
                conn.execute(f"PRAGMA user_version = {SCHEMA_VERSION:d}")
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS channels (
                    channel_id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    enabled INTEGER NOT NULL DEFAULT 1,
                    status TEXT NOT NULL DEFAULT 'running',
                    concurrency INTEGER NOT NULL DEFAULT 1,
                    inbound_transport TEXT NOT NULL,
                    inbound_transport_config TEXT,
                    inbound_codec TEXT NOT NULL,
                    outbound_codec TEXT NOT NULL,
                    destination TEXT NOT NULL,
                    destination_config TEXT,
                    retry_policy_id TEXT,
                    pipeline TEXT NOT NULL DEFAULT '[]'
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS shared_steps (
                    kind TEXT NOT NULL,
                    id TEXT NOT NULL,
                    version INTEGER NOT NULL,
                    config TEXT NOT NULL,
                    description TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (kind, id, version)
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS pipeline_steps (
                    step_id TEXT NOT NULL,
                    channel_id TEXT NOT NULL,
                    version INTEGER NOT NULL,
                    type TEXT NOT NULL,
                    config TEXT NOT NULL,
                    provenance TEXT,
                    description TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (step_id, version)
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_pipeline_steps_channel "
                "ON pipeline_steps (channel_id)"
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS retry_policies (
                    retry_policy_id TEXT PRIMARY KEY,
                    max_retries INTEGER NOT NULL,
                    base_backoff_seconds INTEGER NOT NULL,
                    description TEXT
                )
                """
            )
            conn.commit()
        self.seed_defaults()

    @staticmethod
    def _hard_reset(conn) -> None:
        # Table names come from this fixed tuple — never user input.
        for table in ("channels", "pipeline_steps", "shared_steps",
                      "mappings", "enrichments"):
            conn.execute(f'DROP TABLE IF EXISTS "{table}"')
        conn.commit()

    def seed_defaults(self):
        """Seeds the default retry policy, an identity shared mapping, and a
        demo channel. Only runs when the tables are empty."""
        now = _now_iso()
        with self._get_conn() as conn:
            if conn.execute("SELECT COUNT(*) FROM retry_policies").fetchone()[0] == 0:
                conn.execute(
                    "INSERT INTO retry_policies (retry_policy_id, max_retries, base_backoff_seconds, description) VALUES (?, ?, ?, ?)",
                    ("default", 3, 2, "default: 3 retries, 2s base backoff"),
                )
            if conn.execute("SELECT COUNT(*) FROM shared_steps").fetchone()[0] == 0:
                conn.execute(
                    "INSERT INTO shared_steps (kind, id, version, config, description, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    ("mapping", "identity", 1, json.dumps({"rules": []}),
                     "identity mapping (no changes)", now, now),
                )
            if conn.execute("SELECT COUNT(*) FROM channels").fetchone()[0] == 0:
                conn.execute(
                    """
                    INSERT INTO channels (channel_id, name, enabled, status, concurrency,
                        inbound_transport, inbound_transport_config, inbound_codec, outbound_codec,
                        destination, destination_config, retry_policy_id, pipeline)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    ("his_to_lis", "HIS to LIS demo", 0, "paused", 1,
                     "http_webhook", json.dumps({}), "json", "json", "http",
                     json.dumps({"endpoint_url": "http://localhost:9000/lis"}),
                     "default", "[]"),
                )
            conn.commit()

    # --- versioning / shared steps -----------------------------------------

    def _next_shared_version(self, conn, kind: str, ident: str) -> int:
        row = conn.execute(
            "SELECT COALESCE(MAX(version), 0) + 1 FROM shared_steps WHERE kind = ? AND id = ?",
            (kind, ident),
        ).fetchone()
        return int(row[0])

    def _next_step_version(self, conn, step_id: str) -> int:
        row = conn.execute(
            "SELECT COALESCE(MAX(version), 0) + 1 FROM pipeline_steps WHERE step_id = ?",
            (step_id,),
        ).fetchone()
        return int(row[0])

    def _shared_step_exists(self, kind, ident, version, conn=None) -> bool:
        def _q(c):
            return c.execute(
                "SELECT 1 FROM shared_steps WHERE kind = ? AND id = ? AND version = ?",
                (kind, ident, version),
            ).fetchone()

        if conn is None:
            with self._get_conn() as c:
                return _q(c) is not None
        return _q(conn) is not None

    def _get_shared_step_config(self, kind, ident, version, conn=None):
        def _q(c):
            return c.execute(
                "SELECT config FROM shared_steps WHERE kind = ? AND id = ? AND version = ?",
                (kind, ident, version),
            ).fetchone()

        if conn is None:
            with self._get_conn() as c:
                row = _q(c)
        else:
            row = _q(conn)
        if not row:
            return None
        return self._parse_json(row["config"], {})

    def save_shared_step(self, kind: str, definition_id: str, config: dict,
                         description: str = "", version: int | None = None) -> int:
        """Persists a shared step definition (kind: mapping / enrichment /
        filter / assert). Each save creates a NEW immutable version row;
        channels pin a specific version, so edits never silently alter
        existing consumers."""
        if kind not in SHARED_KINDS:
            raise ConfigValidationError(
                f"unknown shared step kind {kind!r} "
                f"(must be one of {', '.join(SHARED_KINDS)})")
        if not definition_id:
            raise ConfigValidationError("shared step id is required")

        stype = SHARED_KIND_TO_STEP_TYPE[kind]
        cfg = self._normalize_step_config(stype, dict(config or {}))
        errors = self._validate_step_config(
            stype, cfg, f"shared {kind} {definition_id!r}")
        if errors:
            raise ConfigValidationError("; ".join(errors))

        now = _now_iso()
        try:
            with self._get_conn() as conn:
                if version is None:
                    version = self._next_shared_version(conn, kind, definition_id)
                elif self._shared_step_exists(kind, definition_id, version, conn):
                    raise ConfigValidationError(
                        f"shared {kind} {definition_id!r} v{version} already exists — "
                        "shared definitions are immutable; save a new version instead")
                conn.execute(
                    "INSERT INTO shared_steps (kind, id, version, config, description, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (kind, definition_id, version,
                     json.dumps(cfg, default=str), description, now, now),
                )
                conn.commit()
        except sqlite3.IntegrityError as e:
            raise ConfigValidationError(
                f"shared {kind} {definition_id!r} was edited elsewhere — "
                "reload and retry") from e
        return version

    def list_shared_steps(self, kind: str | None = None) -> list[dict]:
        with self._get_conn() as conn:
            if kind:
                rows = conn.execute(
                    "SELECT kind, id, version, description FROM shared_steps "
                    "WHERE kind = ? ORDER BY id, version",
                    (kind,),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT kind, id, version, description FROM shared_steps "
                    "ORDER BY kind, id, version"
                ).fetchall()
        return [dict(r) for r in rows]

    # --- retry policies ----------------------------------------------------

    def save_retry_policy(self, retry_policy_id: str, max_retries: int,
                          base_backoff_seconds: int, description: str = ""):
        if not retry_policy_id:
            raise ConfigValidationError("retry_policy_id is required")
        if int(max_retries) < 0 or int(base_backoff_seconds) < 0:
            raise ConfigValidationError("retry policy values must be >= 0")
        with self._get_conn() as conn:
            conn.execute(
                """
                INSERT INTO retry_policies (retry_policy_id, max_retries, base_backoff_seconds, description)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(retry_policy_id) DO UPDATE SET
                    max_retries = excluded.max_retries,
                    base_backoff_seconds = excluded.base_backoff_seconds,
                    description = excluded.description
                """,
                (retry_policy_id, int(max_retries), int(base_backoff_seconds), description),
            )
            conn.commit()

    def list_retry_policies(self) -> list[dict]:
        with self._get_conn() as conn:
            rows = conn.execute(
                "SELECT retry_policy_id, max_retries, base_backoff_seconds, description FROM retry_policies ORDER BY retry_policy_id"
            ).fetchall()
        return [dict(r) for r in rows]

    def _retry_exists(self, retry_policy_id: str) -> bool:
        with self._get_conn() as conn:
            row = conn.execute(
                "SELECT 1 FROM retry_policies WHERE retry_policy_id = ?", (retry_policy_id,)
            ).fetchone()
        return row is not None

    # --- channel CRUD ------------------------------------------------------

    def save_channel_definition(self, definition: dict) -> None:
        """Validates and persists a channel definition. Invalid definitions
        raise ConfigValidationError and are never saved.

        Steps without a ``step_id`` get one assigned here (stable identity
        across reorders/edits, plan §5 D2); every save appends resolved
        snapshots to the immutable ``pipeline_steps`` history (§5.3).
        """
        definition = dict(definition)
        pipeline = definition.get("pipeline")
        entries: list = []
        if pipeline is not None:
            if not isinstance(pipeline, list):
                raise ConfigValidationError("pipeline must be a JSON array of steps")
            for entry in pipeline:
                e = dict(entry) if isinstance(entry, dict) else entry
                if isinstance(e, dict) and not e.get("step_id"):
                    e["step_id"] = str(uuid.uuid4())
                entries.append(e)
            definition["pipeline"] = entries

        errors = self.validate_channel_definition(definition)
        if errors:
            raise ConfigValidationError("; ".join(errors))

        cid = definition["channel_id"]
        now = _now_iso()
        try:
            with self._get_conn() as conn:
                conn.execute(
                    """
                    INSERT INTO channels (channel_id, name, enabled, status, concurrency,
                        inbound_transport, inbound_transport_config, inbound_codec, outbound_codec,
                        destination, destination_config, retry_policy_id, pipeline)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(channel_id) DO UPDATE SET
                        name = excluded.name, enabled = excluded.enabled, status = excluded.status,
                        concurrency = excluded.concurrency,
                        inbound_transport = excluded.inbound_transport,
                        inbound_transport_config = excluded.inbound_transport_config,
                        inbound_codec = excluded.inbound_codec, outbound_codec = excluded.outbound_codec,
                        destination = excluded.destination, destination_config = excluded.destination_config,
                        retry_policy_id = excluded.retry_policy_id, pipeline = excluded.pipeline
                    """,
                    (
                        cid, definition["name"], 1 if definition.get("enabled", True) else 0,
                        definition.get("status", "running"), int(definition.get("concurrency", 1)),
                        definition["inbound_transport"],
                        _json_text(definition.get("inbound_transport_config")),
                        definition["inbound_codec"], definition["outbound_codec"],
                        definition["destination"], _json_text(definition.get("destination_config")),
                        definition.get("retry_policy_id"),
                        json.dumps(entries, default=str),
                    ),
                )
                for entry in entries:
                    snapshot_cfg, provenance = self._resolve_step_snapshot(entry, conn)
                    version = self._next_step_version(conn, entry["step_id"])
                    conn.execute(
                        "INSERT INTO pipeline_steps (step_id, channel_id, version, type, config, provenance, description, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (entry["step_id"], cid, version, entry["type"],
                         json.dumps(snapshot_cfg, default=str),
                         _json_text(provenance), entry.get("description"), now, now),
                    )
                conn.commit()
        except sqlite3.IntegrityError as e:
            raise ConfigValidationError(
                f"channel {cid!r} was edited elsewhere — reload and retry") from e
        self.load_all_configs()

    def _resolve_step_snapshot(self, entry: dict, conn):
        """Resolve a pipeline entry to (config snapshot, provenance).

        Shared references resolve to the pinned shared config, recorded in
        provenance; inline steps carry no provenance (the channel row is
        the source of truth).
        """
        shared = entry.get("shared")
        if isinstance(shared, dict):
            cfg = self._get_shared_step_config(
                shared.get("kind"), shared.get("id"), shared.get("version"), conn) or {}
            provenance = {
                "kind": shared.get("kind"),
                "id": shared.get("id"),
                "version": shared.get("version"),
            }
        else:
            cfg = entry.get("config") or {}
            provenance = None
        return self._normalize_step_config(entry["type"], cfg), provenance

    def delete_channel(self, channel_id: str) -> None:
        with self._get_conn() as conn:
            conn.execute("DELETE FROM channels WHERE channel_id = ?", (channel_id,))
            conn.execute("DELETE FROM pipeline_steps WHERE channel_id = ?", (channel_id,))
            conn.commit()
        self.load_all_configs()

    # --- validation --------------------------------------------------------

    def validate_channel_definition(self, definition: dict) -> list[str]:
        """Returns a list of validation errors (empty = valid). Unknown
        transports/codecs/destinations, malformed pipeline steps, and
        missing shared/retry references are rejected here, before runtime."""
        errors = []
        if not definition.get("channel_id"):
            errors.append("channel_id is required")
        if not definition.get("name"):
            errors.append("name is required")

        transport = definition.get("inbound_transport") or ""
        if transport not in TRANSPORTS:
            errors.append(f"unknown inbound transport: {transport!r}")

        icodec = definition.get("inbound_codec") or ""
        if icodec not in codec_keys():
            errors.append(f"unknown inbound codec: {icodec!r} (must be an explicit registry key)")
        ocodec = definition.get("outbound_codec") or ""
        if ocodec not in codec_keys():
            errors.append(f"unknown outbound codec: {ocodec!r} (must be an explicit registry key)")

        dest = definition.get("destination") or ""
        if dest not in DESTINATIONS:
            errors.append(f"unknown destination: {dest!r}")

        rp = definition.get("retry_policy_id")
        if not rp:
            errors.append("retry_policy_id is required")
        elif not self._retry_exists(rp):
            errors.append(f"retry policy {rp!r} does not exist")

        pipeline = definition.get("pipeline")
        if pipeline is None:
            pipeline = []
        if not isinstance(pipeline, list):
            errors.append("pipeline must be a JSON array of steps")
        else:
            seen_ids: set[str] = set()
            for i, entry in enumerate(pipeline):
                errors.extend(self._validate_pipeline_entry(i, entry, seen_ids))

        tc = definition.get("inbound_transport_config") or {}
        if transport == "mllp" and not tc.get("port"):
            errors.append("mllp transport requires inbound_transport_config.port")
        if transport == "http_poller" and not tc.get("url"):
            errors.append("http_poller transport requires inbound_transport_config.url")
        if transport == "file_watcher" and not tc.get("directory"):
            errors.append("file_watcher transport requires inbound_transport_config.directory")
        if transport == "db_poller" and (not tc.get("connection_string") or not tc.get("query")):
            errors.append("db_poller transport requires inbound_transport_config.connection_string and query")

        dc = definition.get("destination_config") or {}
        if dest == "http" and not dc.get("endpoint_url"):
            errors.append("http destination requires destination_config.endpoint_url")
        if dest == "mllp" and (not dc.get("host") or not dc.get("port")):
            errors.append("mllp destination requires destination_config.host and port")
        if dest == "sftp" and not dc.get("host"):
            errors.append("sftp destination requires destination_config.host")

        return errors

    def _validate_pipeline_entry(self, index: int, entry, seen_ids: set) -> list[str]:
        label = f"pipeline[{index}]"
        if not isinstance(entry, dict):
            return [f"{label}: step must be a JSON object"]

        errors: list[str] = []
        sid = entry.get("step_id")
        if sid is not None:
            if not isinstance(sid, str) or not _STEP_ID_RE.match(sid):
                errors.append(
                    f"{label}: step_id {sid!r} must match [A-Za-z0-9_-]{{1,64}}")
            elif sid in seen_ids:
                errors.append(f"{label}: duplicate step_id {sid!r}")
            else:
                seen_ids.add(sid)

        stype = entry.get("type")
        if stype not in STEP_TYPES:
            errors.append(
                f"{label}: unknown step type {stype!r} "
                f"(must be one of {', '.join(STEP_TYPES)})")
            return errors

        if entry.get("description") is not None and not isinstance(entry.get("description"), str):
            errors.append(f"{label}: description must be a string")

        has_config = entry.get("config") is not None
        shared = entry.get("shared")
        if has_config and shared:
            errors.append(
                f"{label}: step must have either 'config' (inline) or 'shared' "
                "(reference), not both")
        elif not has_config and not shared:
            errors.append(f"{label}: step requires either inline 'config' or a 'shared' reference")
        elif shared:
            errors.extend(self._validate_shared_ref(label, stype, shared))
        elif not isinstance(entry["config"], dict):
            errors.append(f"{label}: config must be a JSON object")
        else:
            errors.extend(self._validate_step_config(stype, entry["config"], label))
        return errors

    def _validate_shared_ref(self, label: str, stype: str, shared) -> list[str]:
        if not isinstance(shared, dict):
            return [f"{label}: shared reference must be a JSON object"]
        errors: list[str] = []
        kind = shared.get("kind")
        ident = shared.get("id")
        version = shared.get("version")
        expected_kind = STEP_TYPE_TO_SHARED_KIND[stype]
        if kind != expected_kind:
            errors.append(
                f"{label}: shared kind {kind!r} does not match step type {stype!r} "
                f"(expected kind {expected_kind!r})")
        if not ident:
            errors.append(f"{label}: shared reference requires id")
        if not isinstance(version, int) or isinstance(version, bool) or version < 1:
            errors.append(f"{label}: shared reference requires a positive integer version")
        elif kind and ident and not self._shared_step_exists(kind, ident, version):
            errors.append(f"{label}: shared {kind} {ident!r} version {version} does not exist")
        return errors

    def _validate_step_config(self, stype: str, cfg: dict, label: str) -> list[str]:
        """Validate one step's concrete config by dispatching on type."""
        if stype == "enrich":
            return self._validate_enrich_config(cfg, label)
        if stype == "transform":
            return self._validate_transform_config(cfg, label)
        return self._validate_condition_config(stype, cfg, label)

    def _validate_enrich_config(self, cfg: dict, label: str) -> list[str]:
        errors: list[str] = []
        for key in ("source_key_field", "target_table", "target_key_col", "lookup_name"):
            if not cfg.get(key):
                errors.append(f"{label}: enrich requires {key}")

        src = cfg.get("source_key_field")
        if src and not _valid_source_path(str(src)):
            errors.append(
                f"{label}: enrich source_key_field {src!r} is not a known canonical path")

        for key in ("target_table", "target_key_col"):
            val = cfg.get(key)
            if val and not is_valid_identifier(str(val)):
                errors.append(f"{label}: enrich {key}={val!r} is not a valid SQL identifier")

        fields = cfg.get("fields")
        if not isinstance(fields, list) or not fields:
            errors.append(f"{label}: enrich requires a non-empty fields array")
        else:
            for i, fld in enumerate(fields):
                if not isinstance(fld, str) or not is_valid_identifier(fld):
                    errors.append(
                        f"{label}: enrich fields[{i}]={fld!r} is not a valid SQL identifier")

        db_type = (cfg.get("db_type") or "sqlite").lower()
        if db_type not in _ENRICH_DB_TYPES:
            errors.append(
                f"{label}: unsupported db_type {db_type!r} "
                f"(use sqlite, postgresql, or mysql)")
        elif db_type == "sqlite":
            if not cfg.get("lookup_db_path"):
                errors.append(f"{label}: lookup_db_path is required for db_type=sqlite")
        elif not cfg.get("connection_string"):
            errors.append(
                f"{label}: connection_string is required for db_type={db_type!r}")
        return errors

    def _validate_transform_config(self, cfg: dict, label: str) -> list[str]:
        errors: list[str] = []
        rules = cfg.get("rules")
        if not isinstance(rules, list):
            errors.append(f"{label}: transform rules must be a JSON array")
            return errors
        for i, rule in enumerate(rules):
            rlabel = f"{label}: rules[{i}]"
            if not isinstance(rule, dict):
                errors.append(f"{rlabel}: rule must be a JSON object")
                continue
            src = rule.get("source")
            if not src or not isinstance(src, str):
                errors.append(f"{rlabel}: rule requires a source path")
            elif not _valid_source_path(src):
                errors.append(f"{rlabel}: unknown source field {src!r}")
            tgt = rule.get("target")
            if not tgt or not isinstance(tgt, str):
                errors.append(f"{rlabel}: rule requires a target path")
            elif not _valid_target_path(tgt):
                errors.append(f"{rlabel}: unknown target field {tgt!r}")
            elif _canonical_index_zero(tgt) in _IDENTIFIER_OBJECT_PATHS:
                errors.append(
                    f"{rlabel}: target {tgt!r} addresses a whole Identifier "
                    "object — write to .value / .system / .type leaves instead")
            fn = rule.get("fn")
            if fn is not None and fn not in TRANSFORM_FN_REGISTRY:
                errors.append(f"{rlabel}: unknown transform fn {fn!r}")
            if rule.get("fn_args") is not None and not isinstance(rule.get("fn_args"), dict):
                errors.append(f"{rlabel}: fn_args must be a JSON object")
            if rule.get("required") is not None and not isinstance(rule.get("required"), bool):
                errors.append(f"{rlabel}: required must be a boolean")
        return errors

    def _validate_condition_config(self, stype: str, cfg: dict, label: str) -> list[str]:
        """Filter/assert: expression must statically validate against the
        field catalog (``lookups.*`` allowed as a runtime namespace) and
        ``on_fail`` must be within the type's vocabulary (plan §3 D10)."""
        errors: list[str] = []
        expr = cfg.get("expression")
        if not isinstance(expr, dict) or not expr:
            errors.append(f"{label}: {stype} requires an expression object")
        else:
            for e in validate_expression(expr, _CATALOG_PATHS,
                                         allow_prefixes=("lookups.",)):
                errors.append(f"{label}: {e.removeprefix('<root>: ')}")
        allowed = FILTER_ON_FAIL if stype == "filter" else ASSERT_ON_FAIL
        on_fail = cfg.get("on_fail")
        if on_fail is not None and on_fail not in allowed:
            errors.append(
                f"{label}: {stype} on_fail must be one of {', '.join(allowed)} "
                f"(got {on_fail!r})")
        return errors

    @staticmethod
    def _normalize_step_config(stype: str, cfg: dict) -> dict:
        """Fill defaults so resolved step configs are self-describing."""
        cfg = dict(cfg or {})
        if stype == "enrich":
            cfg.setdefault("db_type", "sqlite")
        elif stype in ("filter", "assert"):
            cfg.setdefault("on_fail", "dead_letter")
        return cfg

    @staticmethod
    def _parse_json(value, default=None):
        if value is None:
            return default
        if isinstance(value, (dict, list)):
            return value
        try:
            return json.loads(value)
        except (TypeError, ValueError):
            return default

    # --- loading -----------------------------------------------------------

    def _resolve_pipeline(self, conn, raw) -> list[dict]:
        """Parse the stored ``pipeline`` JSON and resolve shared refs into
        concrete configs. Emits the ordered, resolved step list the runner
        consumes (D4: array order is the ordering authority).

        The original shared reference is kept under ``shared_ref`` for UI
        round-tripping — it is deliberately NOT ``shared``, so a loaded
        config re-validates cleanly as an inline step."""
        resolved = []
        for entry in self._parse_json(raw, []) or []:
            if not isinstance(entry, dict):
                continue
            e = {
                "step_id": entry.get("step_id") or "",
                "type": entry.get("type"),
                "description": entry.get("description"),
            }
            shared = entry.get("shared")
            if isinstance(shared, dict):
                cfg = self._get_shared_step_config(
                    shared.get("kind"), shared.get("id"), shared.get("version"), conn
                ) or {}
                e["shared_ref"] = dict(shared)
            else:
                cfg = entry.get("config") or {}
            e["config"] = self._normalize_step_config(e["type"], cfg)
            resolved.append(e)
        return resolved

    def _row_to_config(self, conn, row) -> dict:
        config = dict(row)
        config["enabled"] = bool(row["enabled"])
        config["inbound_transport_config"] = self._parse_json(row["inbound_transport_config"], {})
        config["destination_config"] = self._parse_json(row["destination_config"], {})
        config["pipeline"] = self._resolve_pipeline(conn, row["pipeline"])

        config["retry_policy"] = {"max_retries": 3, "base_backoff_seconds": 2}
        if row["retry_policy_id"]:
            prow = conn.execute(
                "SELECT max_retries, base_backoff_seconds FROM retry_policies "
                "WHERE retry_policy_id = ?",
                (row["retry_policy_id"],),
            ).fetchone()
            if prow:
                config["retry_policy"] = {
                    "max_retries": prow["max_retries"],
                    "base_backoff_seconds": prow["base_backoff_seconds"],
                }

        return config

    def load_all_configs(self) -> dict:
        configs = {}
        with self._get_conn() as conn:
            for row in conn.execute("SELECT * FROM channels").fetchall():
                configs[row["channel_id"]] = self._row_to_config(conn, row)
        self.configs = configs
        return configs

    def load_config(self, channel_id: str) -> dict | None:
        with self._get_conn() as conn:
            row = conn.execute(
                "SELECT * FROM channels WHERE channel_id = ?", (channel_id,)
            ).fetchone()
        if not row:
            return None
        return self._row_to_config(conn, row)

    # --- builders ----------------------------------------------------------

    def _build_destination(self, config: dict):
        dname = config["destination"]
        dc = config.get("destination_config") or {}
        if dname == "http":
            return HttpClientNode(
                endpoint_url=dc["endpoint_url"],
                method=dc.get("method", "POST"),
                auth=self.auth,
                auth_profile_id=dc.get("auth_profile_id"),
                headers=dc.get("headers"),
                timeout=dc.get("timeout_s", 5),
            )
        if dname == "mllp":
            return MllpClientNode(host=dc["host"], port=dc["port"])
        if dname == "sftp":
            return SFTPClientNode(
                host=dc["host"],
                port=dc.get("port", 22),
                username=dc.get("username", ""),
                password=dc.get("password"),
                private_key_path=dc.get("private_key_path"),
                private_key_passphrase=dc.get("private_key_passphrase"),
                remote_dir=dc.get("remote_dir", "."),
                timeout=dc.get("timeout_s", 10),
            )
        return None

    def _build_enrich_step(self, cfg: dict, step_id: str) -> BatchLookup:
        """Build-time validation + BatchLookup construction for one enrich
        step: the lookup DB must be reachable and the table/columns must
        exist before the channel is allowed to run."""
        db_type = (cfg.get("db_type") or "sqlite").lower()
        label = f"enrich step {step_id or '?'!r}"
        if db_type == "sqlite":
            adapter = build_adapter("sqlite", db_path=cfg["lookup_db_path"])
        else:
            adapter = build_adapter(db_type, connection_string=cfg["connection_string"])

        try:
            adapter.validate()
        except Exception as e:
            raise ConfigValidationError(
                f"{label}: cannot connect to {db_type} db: {e}") from e

        if not adapter.table_exists(cfg["target_table"]):
            raise ConfigValidationError(
                f"{label}: table {cfg['target_table']!r} not found in {db_type} db")

        cols = adapter.column_names(cfg["target_table"])
        if cfg["target_key_col"] not in cols:
            raise ConfigValidationError(
                f"{label}: column {cfg['target_key_col']!r} not found "
                f"in table {cfg['target_table']!r}")
        for fld in cfg.get("fields") or []:
            if fld not in cols:
                raise ConfigValidationError(
                    f"{label}: field {fld!r} not found in table {cfg['target_table']!r}")

        return BatchLookup(
            source_key_field=cfg.get("source_key_field", ""),
            target_table=cfg["target_table"],
            target_key_col=cfg["target_key_col"],
            fields=list(cfg.get("fields") or []),
            lookup_name=cfg.get("lookup_name", ""),
            db_adapter=adapter,
        )

    def _build_pipeline_steps(self, config: dict) -> list[Step]:
        """Builds the ordered runtime ``Step`` chain from a resolved config.
        Enrich steps get full build-time DB validation; structural problems
        surface as ConfigValidationError (fail before runtime)."""
        steps: list[Step] = []
        for entry in config.get("pipeline") or []:
            try:
                step = build_step(entry)
            except ValueError as e:
                raise ConfigValidationError(
                    f"pipeline step {entry.get('step_id')!r}: {e}") from e
            if step.type == "enrich":
                step.impl = self._build_enrich_step(step.config, step.step_id)
            steps.append(step)
        return steps

    def build_runner(self, channel_id: str, queue, destination=None) -> ChannelRunner:
        """Instantiates a ChannelRunner for a channel from its declarative
        config. Validates the config first — invalid channels fail here,
        before runtime. `destination` may override the configured destination
        (test seam)."""
        self.load_all_configs()
        config = self.configs.get(channel_id)
        if not config:
            raise ConfigValidationError(f"no channel configuration for {channel_id!r}")
        errors = self.validate_channel_definition(config)
        if errors:
            raise ConfigValidationError("; ".join(errors))

        steps = self._build_pipeline_steps(config)
        rp = config.get("retry_policy") or {}

        return ChannelRunner(
            channel_id=channel_id,
            queue=queue,
            steps=steps,
            destination=destination if destination is not None else self._build_destination(config),
            inbound_codec=config["inbound_codec"],
            outbound_codec=config["outbound_codec"],
            max_retries=rp.get("max_retries", 3),
            base_backoff=rp.get("base_backoff_seconds", 2),
        )

    def build_ingestion(self, channel_id: str, queue):
        """Builds an active ingestion node for background transports
        ('mllp', 'http_poller', 'file_watcher', 'db_poller'). 'http_webhook'
        channels aren't built here — webhooks live on the shared Flask app's
        WebhookRegistry (see api/app.py)."""
        config = self.configs.get(channel_id)
        if not config:
            return None
        transport = config.get("inbound_transport")
        if transport == "http_webhook":
            return None
        tc = config.get("inbound_transport_config") or {}
        max_queue_depth = tc.get("max_queue_depth")

        inbound_codec = config.get("inbound_codec", "json")

        if transport == "http_poller":
            return HTTPPoller(
                url=tc["url"], channel_id=channel_id, queue=queue, auth=self.auth,
                auth_profile_id=tc.get("auth_profile_id"),
                interval_s=tc.get("interval_s", 10), max_queue_depth=max_queue_depth,
                idempotency_key_field=tc.get("idempotency_key_field"),
                inbound_codec=inbound_codec,
            )
        if transport == "mllp":
            return MLLPServer(
                host=tc.get("host", "0.0.0.0"), port=tc["port"], channel_id=channel_id,
                queue=queue, max_connections=tc.get("max_connections", 20),
                idle_timeout_s=tc.get("idle_timeout_s", 300), max_queue_depth=max_queue_depth,
                idempotency_from_msh10=bool(tc.get("idempotency_from_msh10")),
                inbound_codec=inbound_codec,
            )
        if transport == "file_watcher":
            return FileWatcher(
                directory=tc["directory"], channel_id=channel_id, queue=queue,
                interval_s=tc.get("interval_s", 5),
                extensions=tuple(tc.get("extensions", [".csv", ".hl7", ".txt"])),
                max_queue_depth=max_queue_depth,
                inbound_codec=inbound_codec,
            )
        if transport == "db_poller":
            return DBPoller(
                connection_string=tc["connection_string"], query=tc["query"],
                channel_id=channel_id, queue=queue,
                db_type=tc.get("db_type", "sqlite"), interval_s=tc.get("interval_s", 10),
                cursor_field=tc.get("cursor_field"), cursor_param=tc.get("cursor_param"),
                max_queue_depth=max_queue_depth,
                idempotency_key_field=tc.get("idempotency_key_field"),
                inbound_codec=inbound_codec,
            )
        return None






