import json
import os
import sqlite3
from glob import glob

from engine.runner import ChannelRunner
from nodes.destination.mllp_client import MllpClientNode
from nodes.destination.http_client import HttpClientNode
from nodes.destination.sftp_client import SFTPClientNode
from nodes.transform.field_mapper import FieldMapper
from nodes.enrichment.batch_lookup import BatchLookup
from nodes.ingestion.http_poller import HTTPPoller
from nodes.ingestion.mllp_server import MLLPServer
from nodes.ingestion.file_watcher import FileWatcher
from nodes.ingestion.db_poller import DBPoller
from core.auth_manager import AuthManager

# Columns added on top of the original MVP schema. Kept as a migration list
# (rather than a fresh CREATE TABLE) so existing queue.db files upgrade in
# place instead of needing to be deleted.
_NEW_COLUMNS = {
    "ingestion_type": "TEXT",       # None | "http_poller" | "http_webhook" | "mllp_server" | "file_watcher" | "db_poller"
    "ingestion_config": "TEXT",     # JSON blob, shape depends on ingestion_type
    "enrichment_config": "TEXT",    # JSON blob: {db_path, source_key_field, target_table, target_key_col, fields, lookup_name}
    "concurrency": "INTEGER",       # worker threads draining this channel's queue concurrently (default 1)
}

_auth_manager: AuthManager | None = None


def get_auth_manager() -> AuthManager:
    """Process-wide AuthManager singleton, loaded once from
    config/auth_profiles.json. Never re-read per request — profiles rarely
    change, and re-parsing on every send would be wasted work."""
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
        self.seed_default_channels()

    def _get_conn(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _ensure_schema(self):
        with self._get_conn() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS channels (
                    channel_id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    enabled INTEGER DEFAULT 1,
                    status TEXT DEFAULT 'running',
                    type TEXT,
                    retry_policy TEXT,
                    mapping_rules TEXT,
                    destination TEXT
                )
            """)
            existing_cols = {row["name"] for row in conn.execute("PRAGMA table_info(channels)")}
            for col, coltype in _NEW_COLUMNS.items():
                if col not in existing_cols:
                    conn.execute(f"ALTER TABLE channels ADD COLUMN {col} {coltype}")
            conn.commit()

    def seed_default_channels(self):
        """Seed channels from legacy config files or defaults if database is empty."""
        try:
            with self._get_conn() as conn:
                count = conn.execute("SELECT COUNT(*) FROM channels").fetchone()[0]
                if count > 0:
                    return

                seeded_any = False
                config_dir = "configs"
                if os.path.exists(config_dir):
                    pattern = os.path.join(config_dir, "*.json")
                    for filepath in glob(pattern):
                        try:
                            with open(filepath, "r") as f:
                                config = json.load(f)
                                channel_id = config.get("channel_id")
                                if channel_id:
                                    conn.execute("""
                                        INSERT INTO channels
                                            (channel_id, name, enabled, status, type, retry_policy,
                                             mapping_rules, destination, ingestion_type, ingestion_config,
                                             enrichment_config)
                                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                                    """, (
                                        channel_id,
                                        config.get("name", channel_id),
                                        1 if config.get("enabled", True) else 0,
                                        "running",
                                        "HTTP Outbound" if config.get("destination", {}).get("type") == "http" else "MLLP Outbound",
                                        json.dumps(config.get("retry_policy", {})),
                                        json.dumps(config.get("mapping_rules", [])),
                                        json.dumps(config.get("destination")) if config.get("destination") else None,
                                        config.get("ingestion_type"),
                                        json.dumps(config.get("ingestion_config")) if config.get("ingestion_config") else None,
                                        json.dumps(config.get("enrichment_config")) if config.get("enrichment_config") else None,
                                    ))
                                    seeded_any = True
                        except Exception as e:
                            print(f"[Seed Error] Failed to seed from {filepath}: {e}")
                    conn.commit()

                if not seeded_any:
                    conn.execute("""
                        INSERT INTO channels
                            (channel_id, name, enabled, status, type, retry_policy,
                             mapping_rules, destination, ingestion_type, ingestion_config,
                             enrichment_config)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        "his_to_lis",
                        "HIS to LIS Pipeline",
                        1,
                        "running",
                        "HTTP Webhook -> HTTP Outbound",
                        json.dumps({"max_retries": 3, "base_backoff_seconds": 2}),
                        json.dumps([
                            {"source": "order_id", "target": "accession_num", "required": True},
                            {"source": "doctor_username", "target": "doctor", "required": True, "fn": "Uppercase"},
                            {"source": "test", "target": "test_code", "required": False}
                        ]),
                        json.dumps({"type": "http", "endpoint_url": "http://localhost:5005/api/lis/orders"}),
                        "http_webhook",
                        json.dumps({"shared_secret": None}),
                        None,
                    ))
                conn.commit()
        except Exception as e:
            print(f"[Seed Error] Failed to run database seed check: {e}")

    def load_all_configs(self) -> dict:
        """Parses all channel configurations from the SQLite database."""
        self.configs.clear()
        try:
            with self._get_conn() as conn:
                rows = conn.execute("SELECT * FROM channels").fetchall()
                for row in rows:
                    channel_id = row["channel_id"]
                    row_keys = row.keys()

                    def _load_json(col):
                        if col not in row_keys or not row[col]:
                            return None
                        try:
                            return json.loads(row[col])
                        except Exception:
                            return None

                    self.configs[channel_id] = {
                        "channel_id": channel_id,
                        "name": row["name"],
                        "enabled": bool(row["enabled"]),
                        "status": row["status"],
                        "type": row["type"],
                        "retry_policy": _load_json("retry_policy") or {},
                        "mapping_rules": _load_json("mapping_rules") or [],
                        "destination": _load_json("destination"),
                        "ingestion_type": row["ingestion_type"] if "ingestion_type" in row_keys else None,
                        "ingestion_config": _load_json("ingestion_config") or {},
                        "enrichment_config": _load_json("enrichment_config"),
                        "concurrency": (row["concurrency"] if "concurrency" in row_keys and row["concurrency"] else 1),
                    }
        except Exception as e:
            print(f"[Config Error] Failed to load configs from database: {e}")
        return self.configs

    def load_config(self, channel_id):
        """Fetch/reload configs and return a specific channel configuration."""
        all_configs = self.load_all_configs()
        return all_configs.get(channel_id)

    def build_runner(self, channel_id: str, queue) -> ChannelRunner:
        """Instantiates a ChannelRunner using the channel's database configuration."""
        self.load_all_configs()

        config = self.configs.get(channel_id)
        if not config:
            raise ValueError(f"No configuration found for channel: {channel_id}")

        mapper = FieldMapper(config.get("mapping_rules", []))
        destination = self.build_destination(config.get("destination"))
        enricher = self.build_enrichment(config.get("enrichment_config"))

        retry_policy = config.get("retry_policy", {})
        max_retries = retry_policy.get("max_retries", 3)
        base_backoff = retry_policy.get("base_backoff_seconds", 2)

        return ChannelRunner(
            channel_id=channel_id,
            queue=queue,
            mapper=mapper,
            destination=destination,
            enricher=enricher,
            max_retries=max_retries,
            base_backoff=base_backoff,
        )

    def build_destination(self, dest_config: dict):
        if not dest_config:
            return None
        dest_type = dest_config.get("type")
        if dest_type == "http":
            return HttpClientNode(
                endpoint_url=dest_config.get("endpoint_url"),
                method=dest_config.get("method", "POST"),
                auth=self.auth,
                auth_profile_id=dest_config.get("auth_profile_id"),
                timeout=dest_config.get("timeout_s", 5),
            )
        elif dest_type == "mllp":
            return MllpClientNode(
                host=dest_config.get("host"),
                port=dest_config.get("port"),
            )
        elif dest_type == "sftp":
            return SFTPClientNode(
                host=dest_config.get("host"),
                port=dest_config.get("port", 22),
                username=dest_config.get("username", ""),
                password=dest_config.get("password"),
                private_key_path=dest_config.get("private_key_path"),
                private_key_passphrase=dest_config.get("private_key_passphrase"),
                remote_dir=dest_config.get("remote_dir", "."),
                filename_field=dest_config.get("filename_field"),
                content_field=dest_config.get("content_field"),
                timeout=dest_config.get("timeout_s", 10),
            )
        return None

    def build_enrichment(self, enrichment_config: dict | None):
        if not enrichment_config:
            return None
        return BatchLookup(
            db_path=enrichment_config.get("db_path", self.db_path),
            source_key_field=enrichment_config["source_key_field"],
            target_table=enrichment_config["target_table"],
            target_key_col=enrichment_config["target_key_col"],
            fields=enrichment_config["fields"],
            lookup_name=enrichment_config.get("lookup_name", "lookup"),
        )

    def build_ingestion(self, channel_id: str, queue):
        """Builds an active ingestion node for channels with an ingestion_type
        that runs as a background thread ('http_poller', 'mllp_server',
        'file_watcher'). 'http_webhook' channels aren't built here — they're
        registered onto the shared Flask app's WebhookRegistry instead (see
        api/app.py), since a webhook needs to live on the running web server,
        not a background worker thread."""
        config = self.configs.get(channel_id)
        if not config:
            return None
        itype = config.get("ingestion_type")
        icfg = config.get("ingestion_config") or {}
        max_queue_depth = icfg.get("max_queue_depth")
        idempotency_key_field = icfg.get("idempotency_key_field")

        if itype == "http_poller":
            return HTTPPoller(
                url=icfg["url"],
                channel_id=channel_id,
                queue=queue,
                auth=self.auth,
                auth_profile_id=icfg.get("auth_profile_id"),
                interval_s=icfg.get("interval_s", 10),
                records_path=icfg.get("records_path", ""),
                cursor_param=icfg.get("cursor_param"),
                cursor_field=icfg.get("cursor_field"),
                max_queue_depth=max_queue_depth,
                idempotency_key_field=idempotency_key_field,
            )
        elif itype == "mllp_server":
            return MLLPServer(
                host=icfg.get("host", "0.0.0.0"),
                port=icfg["port"],
                channel_id=channel_id,
                queue=queue,
                max_connections=icfg.get("max_connections", 20),
                idle_timeout_s=icfg.get("idle_timeout_s", 300),
                max_queue_depth=max_queue_depth,
                idempotency_from_msh10=bool(idempotency_key_field),
            )
        elif itype == "file_watcher":
            return FileWatcher(
                directory=icfg["directory"],
                channel_id=channel_id,
                queue=queue,
                interval_s=icfg.get("interval_s", 5),
                extensions=tuple(icfg.get("extensions", [".csv", ".hl7", ".txt"])),
                max_queue_depth=max_queue_depth,
                csv_mode=icfg.get("csv_mode", "auto"),
            )
        elif itype == "db_poller":
            return DBPoller(
                connection_string=icfg["connection_string"],
                query=icfg["query"],
                channel_id=channel_id,
                queue=queue,
                db_type=icfg.get("db_type", "sqlite"),
                interval_s=icfg.get("interval_s", 10),
                cursor_field=icfg.get("cursor_field"),
                cursor_param=icfg.get("cursor_param"),
                max_queue_depth=max_queue_depth,
                idempotency_key_field=idempotency_key_field,
            )
        return None
