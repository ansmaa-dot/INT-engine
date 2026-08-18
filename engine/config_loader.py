"""Channel configuration: declarative channels referencing reusable
capabilities.

A channel is orchestration ONLY:

    Inbound Transport + Inbound Codec + [Enrichment ref] + [Mapping ref]
  + Outbound Codec + Destination + Retry Policy ref [+ optional semantics]

Reusable definitions live in dedicated tables with stable identity/versioning:

  * mappings       — (mapping_id, version) -> JSON rules using canonical paths
  * enrichments    — (enrichment_id, version) -> lookup definition
  * retry_policies — retry_policy_id -> max_retries / base_backoff_seconds

Editing a mapping/enrichment creates a NEW version row; channels pin a
specific version, so a change never silently alters consumers unless they are
explicitly repointed to the new version.

Configuration validation is explicit and fail-fast. Unknown transports,
codecs, or destinations, and missing mapping/enrichment/retry references are
rejected at save/validate time — there is never a silent fallback (e.g. no
implicit passthrough codec).
"""
import json
import sqlite3
from datetime import datetime, timezone

from core.auth_manager import AuthManager
from engine.runner import ChannelRunner
from nodes.codec import keys as codec_keys
from nodes.destination.http_client import HttpClientNode
from nodes.destination.mllp_client import MllpClientNode
from nodes.destination.sftp_client import SFTPClientNode
from nodes.enrichment.batch_lookup import BatchLookup
from nodes.ingestion.db_poller import DBPoller
from nodes.ingestion.file_watcher import FileWatcher
from nodes.ingestion.http_poller import HTTPPoller
from nodes.ingestion.mllp_server import MLLPServer
from nodes.transform.field_mapper import FieldMapper

# Explicit registries of supported transports / destinations. Adding a new
# one is one entry here plus one builder branch below.
TRANSPORTS = {"http_webhook", "mllp", "http_poller", "file_watcher", "db_poller"}
DESTINATIONS = {"http", "mllp", "sftp"}


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
        self.seed_defaults()

    # --- schema ------------------------------------------------------------

    def _get_conn(self):
        conn = sqlite3.connect(self.db_path, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=30000;")
        return conn

    def _ensure_schema(self):
        with self._get_conn() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS channels (
                    channel_id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    enabled INTEGER DEFAULT 1,
                    status TEXT DEFAULT 'running',
                    concurrency INTEGER DEFAULT 1,
                    inbound_transport TEXT NOT NULL,
                    inbound_transport_config TEXT,
                    inbound_codec TEXT NOT NULL,
                    outbound_codec TEXT NOT NULL,
                    destination TEXT NOT NULL,
                    destination_config TEXT,
                    mapping_id TEXT,
                    mapping_version INTEGER,
                    enrichment_id TEXT,
                    enrichment_version INTEGER,
                    retry_policy_id TEXT,
                    semantics TEXT
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS mappings (
                    mapping_id TEXT NOT NULL,
                    version INTEGER NOT NULL,
                    rules TEXT NOT NULL,
                    description TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (mapping_id, version)
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS enrichments (
                    enrichment_id TEXT NOT NULL,
                    version INTEGER NOT NULL,
                    source_key_field TEXT NOT NULL,
                    lookup_db_path TEXT NOT NULL,
                    target_table TEXT NOT NULL,
                    target_key_col TEXT NOT NULL,
                    fields TEXT NOT NULL,
                    lookup_name TEXT NOT NULL,
                    description TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (enrichment_id, version)
                )
                """
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

    def seed_defaults(self):
        """Seeds a demo channel and the reusable definitions it references.
        Only runs when the config tables are empty."""
        with self._get_conn() as conn:
            if conn.execute("SELECT COUNT(*) FROM retry_policies").fetchone()[0] == 0:
                conn.execute(
                    "INSERT INTO retry_policies (retry_policy_id, max_retries, base_backoff_seconds, description) VALUES (?, ?, ?, ?)",
                    ("default", 3, 2, "default: 3 retries, 2s base backoff"),
                )
            if conn.execute("SELECT COUNT(*) FROM mappings").fetchone()[0] == 0:
                conn.execute(
                    "INSERT INTO mappings (mapping_id, version, rules, description, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)",
                    ("identity", 1, "[]", "identity mapping (no changes)", _now_iso(), _now_iso()),
                )
            if conn.execute("SELECT COUNT(*) FROM channels").fetchone()[0] == 0:
                conn.execute(
                    """
                    INSERT INTO channels (channel_id, name, enabled, status, concurrency,
                        inbound_transport, inbound_transport_config, inbound_codec, outbound_codec,
                        destination, destination_config, mapping_id, mapping_version, retry_policy_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        "his_to_lis", "HIS to LIS Pipeline", 1, "running", 1,
                        "http_webhook", json.dumps({"sig_header": "X-Signature"}),
                        "json", "json",
                        "http", json.dumps({"endpoint_url": "http://localhost:5005/api/lis/orders", "method": "POST"}),
                        "identity", 1, "default",
                    ),
                )
            conn.commit()
    # --- reusable definitions CRUD -----------------------------------------

    def _next_version(self, table: str, id_col: str, ident: str) -> int:
        with self._get_conn() as conn:
            row = conn.execute(
                f"SELECT COALESCE(MAX(version), 0) AS v FROM {table} WHERE {id_col} = ?",
                (ident,),
            ).fetchone()
            return int(row["v"]) + 1

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

    def save_mapping(self, mapping_id: str, rules: list, description: str = "",
                     version: int | None = None) -> int:
        """Persists a new mapping version. Each save creates a new version row;
        channels pin a specific version, so edits never silently alter
        existing consumers."""
        if not mapping_id:
            raise ConfigValidationError("mapping_id is required")
        if not isinstance(rules, list):
            raise ConfigValidationError("mapping rules must be a JSON array")
        if version is None:
            version = self._next_version("mappings", "mapping_id", mapping_id)
        now = _now_iso()
        with self._get_conn() as conn:
            conn.execute(
                "INSERT INTO mappings (mapping_id, version, rules, description, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)",
                (mapping_id, version, json.dumps(rules), description, now, now),
            )
            conn.commit()
        return version

    def save_enrichment(self, enrichment_id: str, source_key_field: str,
                        lookup_db_path: str, target_table: str, target_key_col: str,
                        fields: list, lookup_name: str, description: str = "",
                        version: int | None = None) -> int:
        if not enrichment_id:
            raise ConfigValidationError("enrichment_id is required")
        if not all([source_key_field, lookup_db_path, target_table, target_key_col]):
            raise ConfigValidationError("enrichment requires source_key_field, lookup_db_path, target_table, target_key_col")
        if not isinstance(fields, list):
            raise ConfigValidationError("enrichment fields must be a JSON array")
        if version is None:
            version = self._next_version("enrichments", "enrichment_id", enrichment_id)
        now = _now_iso()
        with self._get_conn() as conn:
            conn.execute(
                """
                INSERT INTO enrichments (enrichment_id, version, source_key_field, lookup_db_path,
                    target_table, target_key_col, fields, lookup_name, description, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (enrichment_id, version, source_key_field, lookup_db_path,
                 target_table, target_key_col, json.dumps(fields), lookup_name,
                 description, now, now),
            )
            conn.commit()
        return version

    def list_mappings(self) -> list[dict]:
        with self._get_conn() as conn:
            rows = conn.execute(
                "SELECT mapping_id, version, description FROM mappings ORDER BY mapping_id, version"
            ).fetchall()
        return [dict(r) for r in rows]

    def list_enrichments(self) -> list[dict]:
        with self._get_conn() as conn:
            rows = conn.execute(
                "SELECT enrichment_id, version, description FROM enrichments ORDER BY enrichment_id, version"
            ).fetchall()
        return [dict(r) for r in rows]

    def list_retry_policies(self) -> list[dict]:
        with self._get_conn() as conn:
            rows = conn.execute(
                "SELECT retry_policy_id, max_retries, base_backoff_seconds, description FROM retry_policies ORDER BY retry_policy_id"
            ).fetchall()
        return [dict(r) for r in rows]

    # --- channel CRUD ------------------------------------------------------

    def save_channel_definition(self, definition: dict) -> None:
        """Validates and persists a channel definition. Invalid definitions
        raise ConfigValidationError and are never saved."""
        errors = self.validate_channel_definition(definition)
        if errors:
            raise ConfigValidationError("; ".join(errors))

        cid = definition["channel_id"]
        with self._get_conn() as conn:
            conn.execute(
                """
                INSERT INTO channels (channel_id, name, enabled, status, concurrency,
                    inbound_transport, inbound_transport_config, inbound_codec, outbound_codec,
                    destination, destination_config, mapping_id, mapping_version,
                    enrichment_id, enrichment_version, retry_policy_id, semantics)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(channel_id) DO UPDATE SET
                    name = excluded.name, enabled = excluded.enabled, status = excluded.status,
                    concurrency = excluded.concurrency,
                    inbound_transport = excluded.inbound_transport,
                    inbound_transport_config = excluded.inbound_transport_config,
                    inbound_codec = excluded.inbound_codec, outbound_codec = excluded.outbound_codec,
                    destination = excluded.destination, destination_config = excluded.destination_config,
                    mapping_id = excluded.mapping_id, mapping_version = excluded.mapping_version,
                    enrichment_id = excluded.enrichment_id, enrichment_version = excluded.enrichment_version,
                    retry_policy_id = excluded.retry_policy_id, semantics = excluded.semantics
                """,
                (
                    cid, definition["name"], 1 if definition.get("enabled", True) else 0,
                    definition.get("status", "running"), int(definition.get("concurrency", 1)),
                    definition["inbound_transport"],
                    _json_text(definition.get("inbound_transport_config")),
                    definition["inbound_codec"], definition["outbound_codec"],
                    definition["destination"], _json_text(definition.get("destination_config")),
                    definition.get("mapping_id"), definition.get("mapping_version"),
                    definition.get("enrichment_id"), definition.get("enrichment_version"),
                    definition.get("retry_policy_id"), _json_text(definition.get("semantics")),
                ),
            )
            conn.commit()
        self.load_all_configs()

    def delete_channel(self, channel_id: str) -> None:
        with self._get_conn() as conn:
            conn.execute("DELETE FROM channels WHERE channel_id = ?", (channel_id,))
            conn.commit()
        self.load_all_configs()

    # --- validation --------------------------------------------------------

    def _mapping_exists(self, mapping_id: str, version: int) -> bool:
        with self._get_conn() as conn:
            row = conn.execute(
                "SELECT 1 FROM mappings WHERE mapping_id = ? AND version = ?", (mapping_id, version)
            ).fetchone()
        return row is not None

    def _enrichment_exists(self, enrichment_id: str, version: int) -> bool:
        with self._get_conn() as conn:
            row = conn.execute(
                "SELECT 1 FROM enrichments WHERE enrichment_id = ? AND version = ?",
                (enrichment_id, version),
            ).fetchone()
        return row is not None

    def _retry_exists(self, retry_policy_id: str) -> bool:
        with self._get_conn() as conn:
            row = conn.execute(
                "SELECT 1 FROM retry_policies WHERE retry_policy_id = ?", (retry_policy_id,)
            ).fetchone()
        return row is not None

    def validate_channel_definition(self, definition: dict) -> list[str]:
        """Returns a list of validation errors (empty = valid). Unknown
        transports/codecs/destinations and missing reusable-definition
        references are rejected here, before runtime."""
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

        mapping_id = definition.get("mapping_id")
        if mapping_id:
            v = definition.get("mapping_version")
            if not v:
                errors.append("mapping_version is required when mapping_id is set")
            elif not self._mapping_exists(mapping_id, int(v)):
                errors.append(f"mapping {mapping_id!r} version {v} does not exist")

        enrichment_id = definition.get("enrichment_id")
        if enrichment_id:
            v = definition.get("enrichment_version")
            if not v:
                errors.append("enrichment_version is required when enrichment_id is set")
            elif not self._enrichment_exists(enrichment_id, int(v)):
                errors.append(f"enrichment {enrichment_id!r} version {v} does not exist")

        rp = definition.get("retry_policy_id")
        if not rp:
            errors.append("retry_policy_id is required")
        elif not self._retry_exists(rp):
            errors.append(f"retry policy {rp!r} does not exist")

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

# --- load / build ------------------------------------------------------

    def _parse_json(self, value, default=None):
        if value is None:
            return default
        if isinstance(value, (dict, list)):
            return value
        try:
            return json.loads(value)
        except (TypeError, ValueError):
            return default

    def _row_to_config(self, conn, row) -> dict:
        config = dict(row)
        config["inbound_transport_config"] = self._parse_json(row["inbound_transport_config"], {})
        config["destination_config"] = self._parse_json(row["destination_config"], {})
        config["semantics"] = self._parse_json(row["semantics"], None)

        # resolve reusable references
        config["mapping_rules"] = []
        if row["mapping_id"] and row["mapping_version"]:
            mrow = conn.execute(
                "SELECT rules FROM mappings WHERE mapping_id = ? AND version = ?",
                (row["mapping_id"], row["mapping_version"]),
            ).fetchone()
            if mrow:
                config["mapping_rules"] = self._parse_json(mrow["rules"], [])

        config["enrichment"] = None
        if row["enrichment_id"] and row["enrichment_version"]:
            erow = conn.execute(
                "SELECT * FROM enrichments WHERE enrichment_id = ? AND version = ?",
                (row["enrichment_id"], row["enrichment_version"]),
            ).fetchone()
            if erow:
                config["enrichment"] = dict(erow)

        config["retry_policy"] = {"max_retries": 3, "base_backoff_seconds": 2}
        if row["retry_policy_id"]:
            prow = conn.execute(
                "SELECT max_retries, base_backoff_seconds FROM retry_policies WHERE retry_policy_id = ?",
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
            row = conn.execute("SELECT * FROM channels WHERE channel_id = ?", (channel_id,)).fetchone()
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

    def _build_enrichment(self, config: dict):
        enr = config.get("enrichment")
        if not enr:
            return None
        return BatchLookup(
            db_path=enr["lookup_db_path"],
            source_key_field=enr["source_key_field"],
            target_table=enr["target_table"],
            target_key_col=enr["target_key_col"],
            fields=self._parse_json(enr["fields"], []),
            lookup_name=enr["lookup_name"],
        )

    def build_runner(self, channel_id: str, queue, destination=None) -> ChannelRunner:
        """Instantiates a ChannelRunner for a channel from its declarative
        references. Validates the config first — invalid channels fail here,
        before runtime. `destination` may override the configured destination
        (test seam)."""
        self.load_all_configs()
        config = self.configs.get(channel_id)
        if not config:
            raise ConfigValidationError(f"no channel configuration for {channel_id!r}")
        errors = self.validate_channel_definition(config)
        if errors:
            raise ConfigValidationError("; ".join(errors))

        mapper = FieldMapper(config.get("mapping_rules", [])) if config.get("mapping_id") else None
        enricher = self._build_enrichment(config)
        rp = config.get("retry_policy") or {}

        return ChannelRunner(
            channel_id=channel_id,
            queue=queue,
            mapper=mapper,
            destination=destination if destination is not None else self._build_destination(config),
            enricher=enricher,
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
