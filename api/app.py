import sys
import os
import re
import json
import sqlite3
import html as html_escape
from datetime import datetime, timedelta, timezone
from flask import Flask, render_template, render_template_string, request, jsonify

_esc = html_escape.escape  # shorthand; always escape user-controlled values before f-string interpolation into HTML

CHANNEL_ID_RE = re.compile(r"^[A-Za-z0-9_-]+$")

# Ensure core modules are visible regardless of execution path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.queue import PersistentQueue
from core.message import Envelope
from engine.config_loader import ChannelConfigRegistry, get_auth_manager
from nodes.ingestion.http_webhook import WebhookRegistry

app = Flask(__name__, template_folder="ui/templates")
queue = PersistentQueue("queue.db")
registry = ChannelConfigRegistry("queue.db")
auth_manager = get_auth_manager()

webhooks = WebhookRegistry(queue)
app.register_blueprint(webhooks.bp)


def sync_webhooks():
    """Registers/unregisters webhook routes to match the current channel
    configs. Called at startup and after any create/edit/delete/toggle so the
    live Flask process always matches the database without a restart."""
    configs = registry.load_all_configs()
    live_ids = set()
    for cid, conf in configs.items():
        if conf.get("enabled", True) and conf.get("ingestion_type") == "http_webhook":
            icfg = conf.get("ingestion_config") or {}
            webhooks.register(
                cid,
                shared_secret=icfg.get("shared_secret") or None,
                sig_header=icfg.get("sig_header", "X-Signature"),
                max_queue_depth=icfg.get("max_queue_depth"),
                idempotency_key_field=icfg.get("idempotency_key_field"),
            )
            live_ids.add(cid)
    for cid in list(webhooks._channels.keys()):
        if cid not in live_ids:
            webhooks.unregister(cid)


sync_webhooks()


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/health")
def health():
    configs = registry.load_all_configs()
    running = sum(1 for c in configs.values() if c.get("enabled", True) and c.get("status") == "running")
    return jsonify({"status": "ok", "channels_running": running})


# --- UI ENDPOINTS (HTMX Swaps) ---

def _iso_ago(seconds: float) -> str:
    """ISO timestamp `seconds` in the past — used to bound trailing-window
    throughput queries against audit_log's stored ISO `at` timestamps."""
    return (datetime.now(timezone.utc) - timedelta(seconds=seconds)).isoformat()


def _ago(iso_str: str | None) -> str:
    """Human-friendly relative time ('2m ago') from an ISO timestamp, for
    surfacing last-activity signals in the dashboard."""
    if not iso_str:
        return "never"
    try:
        dt = datetime.fromisoformat(iso_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        diff = (datetime.now(timezone.utc) - dt).total_seconds()
    except Exception:
        return iso_str
    if diff < 0:
        return "just now"
    if diff < 60:
        return f"{int(diff)}s ago"
    if diff < 3600:
        return f"{int(diff // 60)}m ago"
    if diff < 86400:
        return f"{int(diff // 3600)}h ago"
    return f"{int(diff // 86400)}d ago"


def _pretty_payload(raw_str):
    """Render a stored payload (JSON TEXT in `queue`) into a pretty-printed
    form for the split inspector. Non-JSON values are returned verbatim so
    plain-text/HL7 payloads don't get mangled."""
    if raw_str is None:
        return None
    try:
        parsed = json.loads(raw_str)
    except Exception:
        return raw_str
    if isinstance(parsed, (dict, list)):
        return json.dumps(parsed, indent=2, default=str, ensure_ascii=False)
    return str(parsed)


def _payload_preview(raw_str, limit: int = 60) -> str:
    """Short single-line preview of a stored payload for message-list rows."""
    if not raw_str:
        return ""
    try:
        parsed = json.loads(raw_str)
    except Exception:
        text = raw_str
    else:
        if isinstance(parsed, dict):
            text = json.dumps(parsed, default=str)
        else:
            text = str(parsed)
    if len(text) > limit:
        return text[:limit] + "..."
    return text


# Per-message state -> (pill css class, label). Distinct from the per-channel
# load-state pills used further down in the channel table.
_MESSAGE_STATE_PILL = {
    "QUEUED": ("pill-paused", "QUEUED"),
    "PROCESSING": ("pill-paused", "PROCESSING"),
    "DELIVERED": ("pill-running", "DELIVERED"),
    "DEAD_LETTER": ("pill-error", "DEAD-LETTER"),
}


_DEFAULT_HEALTH = {
    "depth": 0, "in_flight": 0, "delivered_cache": 0, "dead_letter": 0,
    "retries": 0, "last_delivered_at": None, "last_error_at": None,
    "delivered_5m": 0, "dnpm": 0.0, "load_state": "ok",
}


def _load_state(depth: int, max_depth) -> str:
    """Queue-depth -> load color. Prefer the channel's configured
    max_queue_depth when present; otherwise fall back to heuristic bands."""
    if max_depth:
        ratio = depth / max_depth
        if ratio >= 0.9:
            return "error"
        if ratio >= 0.5:
            return "warn"
        return "ok"
    if depth >= 200:
        return "error"
    if depth >= 50:
        return "warn"
    return "ok"


def _channel_health(configs: dict) -> dict:
    """Batched per-channel health computed from the queue + audit_log tables
    in a handful of grouped queries (replaces the old per-channel N+1
    queue_depth call). Returns channel_id -> health dict."""
    health = {cid: dict(_DEFAULT_HEALTH) for cid in configs}

    with queue._get_conn() as conn:
        # current-state snapshot per channel from the queue table
        for r in conn.execute("""
            SELECT channel_id,
                SUM(CASE WHEN UPPER(state)='QUEUED' THEN 1 ELSE 0 END) AS depth,
                SUM(CASE WHEN UPPER(state)='PROCESSING' THEN 1 ELSE 0 END) AS in_flight,
                SUM(CASE WHEN UPPER(state)='DELIVERED' THEN 1 ELSE 0 END) AS delivered,
                SUM(CASE WHEN UPPER(state)='DEAD_LETTER' THEN 1 ELSE 0 END) AS dead_letter
            FROM queue GROUP BY channel_id
        """):
            h = health.setdefault(r["channel_id"], dict(_DEFAULT_HEALTH))
            h["depth"] = r["depth"] or 0
            h["in_flight"] = r["in_flight"] or 0
            h["delivered_cache"] = r["delivered"] or 0
            h["dead_letter"] = r["dead_letter"] or 0

        # lifetime signatures from the append-only audit_log
        for r in conn.execute("""
            SELECT channel_id,
                COALESCE(SUM(CASE WHEN event='retry_scheduled' THEN 1 ELSE 0 END), 0) AS retries,
                MAX(CASE WHEN event='delivered' THEN at END) AS last_delivered_at,
                MAX(CASE WHEN event IN ('retry_scheduled','dead_lettered') THEN at END) AS last_error_at
            FROM audit_log GROUP BY channel_id
        """):
            h = health.setdefault(r["channel_id"], dict(_DEFAULT_HEALTH))
            h["retries"] = r["retries"] or 0
            h["last_delivered_at"] = r["last_delivered_at"]
            h["last_error_at"] = r["last_error_at"]

        # delivered throughput over the trailing 5-minute window
        cutoff = _iso_ago(300)
        for r in conn.execute("""
            SELECT channel_id, COUNT(*) AS n
            FROM audit_log WHERE event='delivered' AND at >= ?
            GROUP BY channel_id
        """, (cutoff,)):
            h = health.setdefault(r["channel_id"], dict(_DEFAULT_HEALTH))
            h["delivered_5m"] = r["n"] or 0
            h["dnpm"] = (r["n"] or 0) / 5.0

    for cid, h in health.items():
        max_depth = (configs.get(cid, {}).get("ingestion_config") or {}).get("max_queue_depth")
        load = _load_state(h["depth"], max_depth)
        if h["dead_letter"] > 0:
            load = "error"
        elif h["retries"] > 0 and load == "ok":
            load = "warn"
        h["load_state"] = load

    return health

@app.route("/ui/metrics")
def get_ui_metrics():
    configs = registry.load_all_configs()
    health = _channel_health(configs)

    active_count = sum(1 for c in configs.values() if c.get("status") == "running" and c.get("enabled", True))
    queued = sum(h["depth"] for h in health.values())
    in_flight = sum(h["in_flight"] for h in health.values())
    dlq = sum(h["dead_letter"] for h in health.values())
    retries = sum(h["retries"] for h in health.values())
    delivered = sum(h["delivered_cache"] for h in health.values())
    dnpm = sum(h["dnpm"] for h in health.values())

    degraded = sum(1 for h in health.values() if h["load_state"] != "ok")
    healthy = active_count - degraded

    return render_template_string("""
        <div class="metric-card">
            <div class="val">{{ healthy }}<span class="sub">/ {{ active_count }}</span></div>
            <div class="lbl">Healthy Channels</div>
            <div class="sub">{{ degraded }} degraded</div>
        </div>
        <div class="metric-card">
            <div class="val">{{ queued }}</div>
            <div class="lbl">Queue Depth</div>
            <div class="sub">in-flight: {{ in_flight }}</div>
        </div>
        <div class="metric-card">
            <div class="val" style="color: var(--status-red);">{{ dlq }}</div>
            <div class="lbl">DLQ Count</div>
            <div class="sub">retries across channels: {{ retries }}</div>
        </div>
        <div class="metric-card">
            <div class="val">{{ delivered }}</div>
            <div class="lbl">Delivered Total</div>
            <div class="sub">{{ "%.1f"|format(dnpm) }} / min (5m)</div>
        </div>
    """, healthy=healthy, active_count=active_count, degraded=degraded,
       queued=queued, in_flight=in_flight, dlq=dlq, retries=retries,
       delivered=delivered, dnpm=dnpm)


@app.route("/ui/channels")
def get_ui_channels():
    configs = registry.load_all_configs()
    health = _channel_health(configs)
    rows_html = ""

    load_class = {"ok": "pill-running", "warn": "pill-paused", "error": "pill-error"}
    load_label = {"ok": "OK", "warn": "DEGRADED", "error": "BACKED-UP"}
    depth_color = {"ok": "var(--status-green)", "warn": "var(--status-yellow)", "error": "var(--status-red)"}

    for cid, info in configs.items():
        h = health[cid]
        pill_class = "pill-running" if info.get("status") == "running" else "pill-paused"
        status_label = "Running" if info.get("status") == "running" else "Paused"
        btn_action = "pause" if info.get("status") == "running" else "resume"
        btn_label = "Pause" if info.get("status") == "running" else "Resume"
        btn_class = "btn" if info.get("status") == "running" else "btn btn-primary"

        ingestion_type = info.get("ingestion_type") or "manual"

        if h["last_error_at"] and (not h["last_delivered_at"] or h["last_error_at"] > h["last_delivered_at"]):
            last_line = f'<span style="color: var(--status-red);">last error {_ago(h["last_error_at"])}</span>'
        elif h["last_delivered_at"]:
            last_line = f'delivered {_ago(h["last_delivered_at"])} · {h["dnpm"]:.1f}/min'
        else:
            last_line = "no activity yet"

        # All values below can originate from user-editable channel config
        # (name, id, type strings), so every one is HTML-escaped before
        # being spliced into the f-string — this template is NOT Jinja and
        # gets no auto-escaping.
        safe_cid = _esc(cid, quote=True)
        safe_name = _esc(info.get("name") or cid)
        safe_ingestion_type = _esc(ingestion_type)
        safe_type = _esc(info.get("type") or "N/A")

        rows_html += f"""
        <tr>
            <td>
                <a class="entry-link" href="/channels/{safe_cid}/messages" title="Open channel messages"><strong>{safe_name}</strong></a> <span class="pill {load_class[h['load_state']]}">{load_label[h['load_state']]}</span><br>
                <code style="font-size: 11px; color: var(--accent-blue);">{safe_cid}</code><br>
                <small style="color: #64748b;">in: {safe_ingestion_type} · out: {safe_type}</small><br>
                <small>queue: <b style="color:{depth_color[h['load_state']]};">{h['depth']}</b> · retries: {h['retries']} · dlq: {h['dead_letter']} · in-flight: {h['in_flight']}</small><br>
                <small style="color: #475569;">{last_line}</small>
            </td>
            <td><span class="pill {pill_class}">{status_label}</span></td>
            <td>
                <div style="display: flex; gap: 4px; flex-wrap: wrap;">
                    <button class="{btn_class}"
                            hx-post="/ui/channels/{safe_cid}/{btn_action}"
                            hx-target="#channel-table-body">
                        {btn_label}
                    </button>
                    <button class="btn"
                            hx-get="/ui/channels/{safe_cid}/edit"
                            hx-target="#editor-container"
                            hx-on:htmx:after-request="if(event.detail.successful) switchTab('editor')">
                        Edit
                    </button>
                    <button class="btn btn-danger"
                            hx-delete="/ui/channels/{safe_cid}"
                            hx-confirm="Are you sure you want to delete channel &#39;{safe_cid}&#39;?"
                            hx-target="#channel-table-body">
                        Delete
                    </button>
                </div>
            </td>
        </tr>
        """
    if not rows_html:
        return '<tr><td colspan="3" style="text-align:center; color: var(--text-muted); padding: 12px;">No channels configured. Click "+ New Channel" to create one.</td></tr>'
    return rows_html


@app.route("/ui/channels/<channel_id>/<action>", methods=["POST"])
def toggle_channel(channel_id, action):
    status = "paused" if action == "pause" else "running"
    try:
        with queue._get_conn() as conn:
            conn.execute("UPDATE channels SET status = ? WHERE channel_id = ?", (status, channel_id))
            conn.commit()
    except Exception as e:
        print(f"[Error] Failed to toggle channel status: {e}")

    registry.load_all_configs()
    sync_webhooks()
    return get_ui_channels()


@app.route("/ui/channels/new")
def new_channel_form():
    return render_channel_form()


@app.route("/ui/channels/<channel_id>/edit")
def edit_channel_form(channel_id):
    configs = registry.load_all_configs()
    config = configs.get(channel_id)
    if not config:
        return '<div style="color: var(--status-red); font-weight: bold;">Channel not found</div>', 404

    return render_channel_form(config)


def _auth_profile_options(selected):
    opts = ['<option value="">None</option>']
    for pid in auth_manager.list_profile_ids():
        sel = "selected" if pid == selected else ""
        opts.append(f'<option value="{pid}" {sel}>{pid}</option>')
    return "\n".join(opts)


def render_channel_form(config=None):
    if config is None:
        config = {
            "channel_id": "",
            "name": "",
            "type": "HTTP Outbound",
            "enabled": True,
            "status": "running",
            "retry_policy": {"max_retries": 3, "base_backoff_seconds": 2},
            "mapping_rules": [],
            "destination": {"type": "http", "endpoint_url": ""},
            "ingestion_type": None,
            "ingestion_config": {},
            "enrichment_config": None,
        }

    mapping_rules_json = json.dumps(config.get("mapping_rules", []), indent=2)
    enrichment_json = json.dumps(config.get("enrichment_config"), indent=2) if config.get("enrichment_config") else ""

    dest = config.get("destination") or {"type": "http", "endpoint_url": ""}
    dest_type = dest.get("type", "http")
    dest_http_url = dest.get("endpoint_url", "") if dest_type == "http" else ""
    dest_http_method = dest.get("method", "POST") if dest_type == "http" else "POST"
    dest_http_auth = dest.get("auth_profile_id", "") if dest_type == "http" else ""
    dest_mllp_host = dest.get("host", "") if dest_type == "mllp" else ""
    dest_mllp_port = dest.get("port", "") if dest_type == "mllp" else ""
    dest_sftp_host = dest.get("host", "") if dest_type == "sftp" else ""
    dest_sftp_port = dest.get("port", 22) if dest_type == "sftp" else 22
    dest_sftp_username = dest.get("username", "") if dest_type == "sftp" else ""
    dest_sftp_password = dest.get("password", "") if dest_type == "sftp" else ""
    dest_sftp_key_path = dest.get("private_key_path", "") if dest_type == "sftp" else ""
    dest_sftp_key_pass = dest.get("private_key_passphrase", "") if dest_type == "sftp" else ""
    dest_sftp_remote_dir = dest.get("remote_dir", ".") if dest_type == "sftp" else "."
    dest_sftp_filename_field = dest.get("filename_field", "") if dest_type == "sftp" else ""
    dest_sftp_content_field = dest.get("content_field", "") if dest_type == "sftp" else ""

    max_retries = config.get("retry_policy", {}).get("max_retries", 3)
    base_backoff = config.get("retry_policy", {}).get("base_backoff_seconds", 2)

    ing_type = config.get("ingestion_type") or "none"
    icfg = config.get("ingestion_config") or {}
    ing_url = icfg.get("url", "")
    ing_interval = icfg.get("interval_s", 10)
    ing_records_path = icfg.get("records_path", "")
    ing_cursor_param = icfg.get("cursor_param", "")
    ing_cursor_field = icfg.get("cursor_field", "")
    ing_auth = icfg.get("auth_profile_id", "")
    ing_secret = icfg.get("shared_secret", "") or ""
    ing_sig_header = icfg.get("sig_header", "X-Signature")
    ing_max_queue_depth = icfg.get("max_queue_depth", "")
    ing_idempotency_field = icfg.get("idempotency_key_field", "")
    ing_mllp_host = icfg.get("host", "0.0.0.0")
    ing_mllp_port = icfg.get("port", "")
    ing_mllp_max_conn = icfg.get("max_connections", 20)
    ing_mllp_idle_timeout = icfg.get("idle_timeout_s", 300)
    ing_fw_directory = icfg.get("directory", "")
    ing_fw_interval = icfg.get("interval_s", 5) if ing_type == "file_watcher" else 5
    ing_fw_extensions = ",".join(icfg.get("extensions", [".csv", ".hl7", ".txt"])) if ing_type == "file_watcher" else ".csv,.hl7,.txt"
    ing_db_conn = icfg.get("connection_string", "") if ing_type == "db_poller" else ""
    ing_db_query = icfg.get("query", "") if ing_type == "db_poller" else ""
    ing_db_type = icfg.get("db_type", "sqlite") if ing_type == "db_poller" else "sqlite"
    ing_db_interval = icfg.get("interval_s", 10) if ing_type == "db_poller" else 10
    ing_db_cursor_field = icfg.get("cursor_field", "") if ing_type == "db_poller" else ""
    ing_db_cursor_param = icfg.get("cursor_param", "") if ing_type == "db_poller" else ""
    concurrency = config.get("concurrency") or 1

    channel_id = config["channel_id"]
    is_edit = bool(channel_id)
    readonly_attr = "readonly style='background-color: #cbd5e1; color: #475569;'" if is_edit else ""
    webhook_url_hint = f'<small style="color: var(--text-muted); display:block; margin-top:4px;">POST endpoint: <code>/webhooks/{channel_id}</code></small>' if is_edit else '<small style="color: var(--text-muted); display:block; margin-top:4px;">Save the channel first to see its webhook URL.</small>'

    return f"""
    <form hx-post="/ui/channels/save" hx-target="#form-flash" hx-swap="innerHTML"
          hx-on:htmx:after-request="if(event.detail.successful) switchTab('dashboard')">
        <div id="form-flash"></div>
        <input type="hidden" name="is_edit" value="{"true" if is_edit else "false"}">

        <!-- Channel sub-tabs: Basic -> Source -> Destination -->
        <div class="tab-bar" style="margin-bottom: 12px;">
            <div class="tab active" id="ctab-btn-basic" onclick="switchChannelTab('basic')">Basic</div>
            <div class="tab" id="ctab-btn-source" onclick="switchChannelTab('source')">Source</div>
            <div class="tab" id="ctab-btn-destination" onclick="switchChannelTab('destination')">Destination</div>
        </div>

        <div id="ctab-panel-basic">
        <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 12px; margin-bottom: 12px;">
            <div>
                <label style="display: block; font-weight: bold; margin-bottom: 4px;">Channel ID *</label>
                <input type="text" name="channel_id" value="{channel_id}" required {readonly_attr}
                       placeholder="e.g. ehr_to_billing"
                       style="width: 100%; padding: 6px; border: 1px solid #cbd5e1; border-radius: 3px;">
                <small style="color: var(--text-muted);">Unique identifier. Cannot be changed once created.</small>
            </div>

            <div>
                <label style="display: block; font-weight: bold; margin-bottom: 4px;">Channel Name *</label>
                <input type="text" name="name" value="{config['name']}" required
                       placeholder="e.g. EHR to Billing Sync"
                       style="width: 100%; padding: 6px; border: 1px solid #cbd5e1; border-radius: 3px;">
            </div>
        </div>

        <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 12px; margin-bottom: 12px;">
            <div>
                <label style="display: block; font-weight: bold; margin-bottom: 4px;">Channel Type / Metadata</label>
                <input type="text" name="type" value="{config.get('type') or ''}"
                       placeholder="e.g. HTTP Webhook -> HTTP Outbound"
                       style="width: 100%; padding: 6px; border: 1px solid #cbd5e1; border-radius: 3px;">
            </div>

            <div>
                <label style="display: block; font-weight: bold; margin-bottom: 4px;">Initial State</label>
                <select name="status" style="width: 100%; padding: 6px; border: 1px solid #cbd5e1; border-radius: 3px;">
                    <option value="running" {"selected" if config['status'] == "running" else ""}>Running</option>
                    <option value="paused" {"selected" if config['status'] == "paused" else ""}>Paused</option>
                </select>
            </div>
        </div>
        </div><!-- /ctab-panel-basic -->

        <div id="ctab-panel-source" style="display: none;">
        <div class="section-box" style="margin-bottom: 12px;">
            <h2>Ingestion (Case 1 — Data Inbound)</h2>
            <div class="section-body">
                <div style="margin-bottom: 8px;">
                    <label style="display: block; font-weight: bold; margin-bottom: 4px;">Ingestion Protocol</label>
                    <select name="ingestion_type" id="ingestion_type" onchange="toggleIngestFields()"
                            style="width: 100%; padding: 6px; border: 1px solid #cbd5e1; border-radius: 3px;">
                        <option value="none" {"selected" if ing_type not in ["http_poller", "http_webhook", "mllp_server", "file_watcher", "db_poller"] else ""}>None (manual / POST to /api/ingest)</option>
                        <option value="http_poller" {"selected" if ing_type == "http_poller" else ""}>HTTP Poller (pull)</option>
                        <option value="http_webhook" {"selected" if ing_type == "http_webhook" else ""}>HTTP Webhook (push)</option>
                        <option value="mllp_server" {"selected" if ing_type == "mllp_server" else ""}>MLLP Server (HL7 inbound)</option>
                        <option value="file_watcher" {"selected" if ing_type == "file_watcher" else ""}>File Watcher (CSV/HL7/TXT drop folder)</option>
                        <option value="db_poller" {"selected" if ing_type == "db_poller" else ""}>DB Poller (SQL/table pull)</option>
                    </select>
                </div>

                <div id="ing_common_fields" style="display: {"none" if ing_type == "none" else "block"}; margin-bottom: 8px;">
                    <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 12px;">
                        <div>
                            <label style="display: block; font-weight: bold; margin-bottom: 4px;">Max Queue Depth (backpressure, blank = unbounded)</label>
                            <input type="number" name="ing_max_queue_depth" value="{ing_max_queue_depth}" min="1" placeholder="e.g. 500"
                                   style="width: 100%; padding: 6px; border: 1px solid #cbd5e1; border-radius: 3px;">
                            <small style="color: var(--text-muted);">New messages are rejected/paused once the queue reaches this size, until it drains.</small>
                        </div>
                        <div>
                            <label style="display: block; font-weight: bold; margin-bottom: 4px;">Idempotency Key Field (blank = disabled)</label>
                            <input type="text" name="ing_idempotency_field" value="{ing_idempotency_field}" placeholder="order_id"
                                   style="width: 100%; padding: 6px; border: 1px solid #cbd5e1; border-radius: 3px;">
                            <small style="color: var(--text-muted);">For MLLP, leave blank to disable or set any value to key off MSH-10 automatically.</small>
                        </div>
                    </div>
                </div>

                <div id="ing_poller_fields" style="display: {"block" if ing_type == "http_poller" else "none"};">
                    <div style="margin-bottom: 8px;">
                        <label style="display: block; font-weight: bold; margin-bottom: 4px;">Poll URL</label>
                        <input type="url" name="ing_url" value="{ing_url}" placeholder="https://example.com/api/orders"
                               style="width: 100%; padding: 6px; border: 1px solid #cbd5e1; border-radius: 3px;">
                    </div>
                    <div style="display: grid; grid-template-columns: 1fr 2fr; gap: 12px; margin-bottom: 8px;">
                        <div>
                            <label style="display: block; font-weight: bold; margin-bottom: 4px;">Interval (s, min 5)</label>
                            <input type="number" name="ing_interval" value="{ing_interval}" min="5"
                                   style="width: 100%; padding: 6px; border: 1px solid #cbd5e1; border-radius: 3px;">
                        </div>
                        <div>
                            <label style="display: block; font-weight: bold; margin-bottom: 4px;">Records Path (dotted, blank = root array)</label>
                            <input type="text" name="ing_records_path" value="{ing_records_path}" placeholder="data.orders"
                                   style="width: 100%; padding: 6px; border: 1px solid #cbd5e1; border-radius: 3px;">
                        </div>
                    </div>
                    <div style="display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 12px; margin-bottom: 8px;">
                        <div>
                            <label style="display: block; font-weight: bold; margin-bottom: 4px;">Cursor Param</label>
                            <input type="text" name="ing_cursor_param" value="{ing_cursor_param}" placeholder="since"
                                   style="width: 100%; padding: 6px; border: 1px solid #cbd5e1; border-radius: 3px;">
                        </div>
                        <div>
                            <label style="display: block; font-weight: bold; margin-bottom: 4px;">Cursor Field</label>
                            <input type="text" name="ing_cursor_field" value="{ing_cursor_field}" placeholder="id"
                                   style="width: 100%; padding: 6px; border: 1px solid #cbd5e1; border-radius: 3px;">
                        </div>
                        <div>
                            <label style="display: block; font-weight: bold; margin-bottom: 4px;">Auth Profile</label>
                            <select name="ing_auth_profile" style="width: 100%; padding: 6px; border: 1px solid #cbd5e1; border-radius: 3px;">
                                {_auth_profile_options(ing_auth)}
                            </select>
                        </div>
                    </div>
                </div>

                <div id="ing_webhook_fields" style="display: {"block" if ing_type == "http_webhook" else "none"};">
                    <div style="display: grid; grid-template-columns: 2fr 1fr; gap: 12px;">
                        <div>
                            <label style="display: block; font-weight: bold; margin-bottom: 4px;">Shared Secret (optional, enables HMAC verification)</label>
                            <input type="text" name="ing_secret" value="{ing_secret}" placeholder="leave blank to accept unsigned requests"
                                   style="width: 100%; padding: 6px; border: 1px solid #cbd5e1; border-radius: 3px;">
                        </div>
                        <div>
                            <label style="display: block; font-weight: bold; margin-bottom: 4px;">Signature Header</label>
                            <input type="text" name="ing_sig_header" value="{ing_sig_header}"
                                   style="width: 100%; padding: 6px; border: 1px solid #cbd5e1; border-radius: 3px;">
                        </div>
                    </div>
                    {webhook_url_hint}
                </div>

                <div id="ing_mllp_fields" style="display: {"block" if ing_type == "mllp_server" else "none"};">
                    <div style="display: grid; grid-template-columns: 2fr 1fr 1fr 1fr; gap: 12px;">
                        <div>
                            <label style="display: block; font-weight: bold; margin-bottom: 4px;">Bind Host</label>
                            <input type="text" name="ing_mllp_host" value="{ing_mllp_host}" placeholder="0.0.0.0"
                                   style="width: 100%; padding: 6px; border: 1px solid #cbd5e1; border-radius: 3px;">
                        </div>
                        <div>
                            <label style="display: block; font-weight: bold; margin-bottom: 4px;">Port</label>
                            <input type="number" name="ing_mllp_port" value="{ing_mllp_port}" placeholder="6001"
                                   style="width: 100%; padding: 6px; border: 1px solid #cbd5e1; border-radius: 3px;">
                        </div>
                        <div>
                            <label style="display: block; font-weight: bold; margin-bottom: 4px;">Max Connections</label>
                            <input type="number" name="ing_mllp_max_conn" value="{ing_mllp_max_conn}" min="1"
                                   style="width: 100%; padding: 6px; border: 1px solid #cbd5e1; border-radius: 3px;">
                        </div>
                        <div>
                            <label style="display: block; font-weight: bold; margin-bottom: 4px;">Idle Timeout (s)</label>
                            <input type="number" name="ing_mllp_idle_timeout" value="{ing_mllp_idle_timeout}" min="10"
                                   style="width: 100%; padding: 6px; border: 1px solid #cbd5e1; border-radius: 3px;">
                        </div>
                    </div>
                    <small style="color: var(--text-muted); display:block; margin-top:4px;">Raw HL7 over MLLP. Message is persisted to the queue before the AA/AE ACK is sent back, so a crash never silently loses a message the sender thinks was delivered.</small>
                </div>

                <div id="ing_fw_fields" style="display: {"block" if ing_type == "file_watcher" else "none"};">
                    <div style="margin-bottom: 8px;">
                        <label style="display: block; font-weight: bold; margin-bottom: 4px;">Watch Directory</label>
                        <input type="text" name="ing_fw_directory" value="{ing_fw_directory}" placeholder="/data/inbound/ehr_exports"
                               style="width: 100%; padding: 6px; border: 1px solid #cbd5e1; border-radius: 3px;">
                    </div>
                    <div style="display: grid; grid-template-columns: 1fr 2fr; gap: 12px;">
                        <div>
                            <label style="display: block; font-weight: bold; margin-bottom: 4px;">Scan Interval (s)</label>
                            <input type="number" name="ing_fw_interval" value="{ing_fw_interval}" min="1"
                                   style="width: 100%; padding: 6px; border: 1px solid #cbd5e1; border-radius: 3px;">
                        </div>
                        <div>
                            <label style="display: block; font-weight: bold; margin-bottom: 4px;">Extensions (comma-separated)</label>
                            <input type="text" name="ing_fw_extensions" value="{ing_fw_extensions}"
                                   style="width: 100%; padding: 6px; border: 1px solid #cbd5e1; border-radius: 3px;">
                        </div>
                    </div>
                    <small style="color: var(--text-muted); display:block; margin-top:4px;">Processed files move to <code><directory>/processed</code>, failures to <code><directory>/failed</code>. .csv files are read row-by-row; .hl7/.txt are enqueued whole.</small>
                </div>

                <div id="ing_db_fields" style="display: {"block" if ing_type == "db_poller" else "none"};">
                    <div style="margin-bottom: 8px;">
                        <label style="display: block; font-weight: bold; margin-bottom: 4px;">Connection String</label>
                        <input type="text" name="ing_db_conn" value="{ing_db_conn}" placeholder="sqlite:///orders.db"
                               style="width: 100%; padding: 6px; border: 1px solid #cbd5e1; border-radius: 3px;">
                        <small style="color: var(--text-muted);">SQLAlchemy URL: sqlite:///path.db, postgresql://user:pass@host/db, mysql+pymysql://user:pass@host/db</small>
                    </div>
                    <div style="margin-bottom: 8px;">
                        <label style="display: block; font-weight: bold; margin-bottom: 4px;">Query</label>
                        <textarea name="ing_db_query" rows="3" placeholder="SELECT * FROM orders WHERE status = 'pending'"
                                  style="width: 100%; padding: 6px; border: 1px solid #cbd5e1; border-radius: 3px; font-family: var(--code-font); font-size: 11px; white-space: pre;">{ing_db_query}</textarea>
                    </div>
                    <div style="display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 12px; margin-bottom: 8px;">
                        <div>
                            <label style="display: block; font-weight: bold; margin-bottom: 4px;">DB Type</label>
                            <select name="ing_db_type" style="width: 100%; padding: 6px; border: 1px solid #cbd5e1; border-radius: 3px;">
                                <option value="sqlite" {"selected" if ing_db_type == "sqlite" else ""}>SQLite</option>
                                <option value="postgresql" {"selected" if ing_db_type == "postgresql" else ""}>PostgreSQL</option>
                                <option value="mysql" {"selected" if ing_db_type == "mysql" else ""}>MySQL</option>
                            </select>
                        </div>
                        <div>
                            <label style="display: block; font-weight: bold; margin-bottom: 4px;">Interval (s, min 5)</label>
                            <input type="number" name="ing_db_interval" value="{ing_db_interval}" min="5"
                                   style="width: 100%; padding: 6px; border: 1px solid #cbd5e1; border-radius: 3px;">
                        </div>
                        <div>
                            <label style="display: block; font-weight: bold; margin-bottom: 4px;">Cursor Field</label>
                            <input type="text" name="ing_db_cursor_field" value="{ing_db_cursor_field}" placeholder="id"
                                   style="width: 100%; padding: 6px; border: 1px solid #cbd5e1; border-radius: 3px;">
                        </div>
                    </div>
                    <div style="display: grid; grid-template-columns: 1fr; gap: 12px;">
                        <div>
                            <label style="display: block; font-weight: bold; margin-bottom: 4px;">Cursor Param (bound variable in query)</label>
                            <input type="text" name="ing_db_cursor_param" value="{ing_db_cursor_param}" placeholder=":last_id"
                                   style="width: 100%; padding: 6px; border: 1px solid #cbd5e1; border-radius: 3px;">
                            <small style="color: var(--text-muted);">Use <code>:cursor</code> in the query to reference the last seen cursor value, e.g. <code>WHERE id > :cursor</code>.</small>
                        </div>
                    </div>
                </div>
            </div>
        </div>
        </div><!-- /ctab-panel-source -->

        <div id="ctab-panel-destination" style="display: none;">
        <div class="section-box" style="margin-bottom: 12px;">
            <h2>Enrichment (Case 2 — optional batch lookup)</h2>
            <div class="section-body">
                <textarea name="enrichment_config" rows="5"
                          placeholder='{{"db_path": "queue.db", "source_key_field": "patient_id", "target_table": "patients", "target_key_col": "patient_id", "fields": ["first_name","last_name"], "lookup_name": "patient"}}'
                          style="width: 100%; padding: 6px; border: 1px solid #cbd5e1; border-radius: 3px; font-family: var(--code-font); font-size: 11px; white-space: pre;">{enrichment_json}</textarea>
                <small style="color: var(--text-muted);">Leave blank to skip enrichment. Reference this lookup in mapping rules as <code>lookups.&lt;lookup_name&gt;.&lt;field&gt;</code>.</small>
            </div>
        </div>

        <div class="section-box" style="margin-bottom: 12px;">
            <h2>Destination Config</h2>
            <div class="section-body">
                <div style="margin-bottom: 8px;">
                    <label style="display: block; font-weight: bold; margin-bottom: 4px;">Destination Protocol</label>
                    <select name="dest_protocol" id="dest_protocol" onchange="toggleDestFields()"
                            style="width: 100%; padding: 6px; border: 1px solid #cbd5e1; border-radius: 3px;">
                        <option value="none" {"selected" if dest_type not in ["http", "mllp", "sftp"] else ""}>None (Ingestion Only)</option>
                        <option value="http" {"selected" if dest_type == "http" else ""}>HTTP Outbound</option>
                        <option value="mllp" {"selected" if dest_type == "mllp" else ""}>MLLP Outbound</option>
                        <option value="sftp" {"selected" if dest_type == "sftp" else ""}>SFTP Outbound</option>
                    </select>
                </div>

                <div id="dest_http_fields" style="display: {"block" if dest_type == "http" else "none"}; margin-bottom: 8px;">
                    <div style="margin-bottom: 8px;">
                        <label style="display: block; font-weight: bold; margin-bottom: 4px;">HTTP Endpoint URL</label>
                        <input type="url" name="dest_http_url" value="{dest_http_url}"
                               placeholder="http://localhost:5005/api/lis/orders"
                               style="width: 100%; padding: 6px; border: 1px solid #cbd5e1; border-radius: 3px;">
                    </div>
                    <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 12px;">
                        <div>
                            <label style="display: block; font-weight: bold; margin-bottom: 4px;">Method</label>
                            <select name="dest_http_method" style="width: 100%; padding: 6px; border: 1px solid #cbd5e1; border-radius: 3px;">
                                <option value="POST" {"selected" if dest_http_method == "POST" else ""}>POST</option>
                                <option value="PUT" {"selected" if dest_http_method == "PUT" else ""}>PUT</option>
                                <option value="PATCH" {"selected" if dest_http_method == "PATCH" else ""}>PATCH</option>
                            </select>
                        </div>
                        <div>
                            <label style="display: block; font-weight: bold; margin-bottom: 4px;">Auth Profile</label>
                            <select name="dest_http_auth" style="width: 100%; padding: 6px; border: 1px solid #cbd5e1; border-radius: 3px;">
                                {_auth_profile_options(dest_http_auth)}
                            </select>
                        </div>
                    </div>
                </div>

                <div id="dest_mllp_fields" style="display: {"block" if dest_type == "mllp" else "none"};">
                    <div style="display: grid; grid-template-columns: 3fr 1fr; gap: 12px;">
                        <div>
                            <label style="display: block; font-weight: bold; margin-bottom: 4px;">MLLP Host</label>
                            <input type="text" name="dest_mllp_host" value="{dest_mllp_host}"
                                   placeholder="127.0.0.1"
                                   style="width: 100%; padding: 6px; border: 1px solid #cbd5e1; border-radius: 3px;">
                        </div>
                        <div>
                            <label style="display: block; font-weight: bold; margin-bottom: 4px;">Port</label>
                            <input type="number" name="dest_mllp_port" value="{dest_mllp_port or ''}"
                                   placeholder="5000"
                                   style="width: 100%; padding: 6px; border: 1px solid #cbd5e1; border-radius: 3px;">
                        </div>
                    </div>
                </div>

                <div id="dest_sftp_fields" style="display: {"block" if dest_type == "sftp" else "none"};">
                    <div style="display: grid; grid-template-columns: 3fr 1fr; gap: 12px; margin-bottom: 8px;">
                        <div>
                            <label style="display: block; font-weight: bold; margin-bottom: 4px;">SFTP Host</label>
                            <input type="text" name="dest_sftp_host" value="{dest_sftp_host}"
                                   placeholder="sftp.example.com"
                                   style="width: 100%; padding: 6px; border: 1px solid #cbd5e1; border-radius: 3px;">
                        </div>
                        <div>
                            <label style="display: block; font-weight: bold; margin-bottom: 4px;">Port</label>
                            <input type="number" name="dest_sftp_port" value="{dest_sftp_port}"
                                   placeholder="22"
                                   style="width: 100%; padding: 6px; border: 1px solid #cbd5e1; border-radius: 3px;">
                        </div>
                    </div>
                    <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 12px; margin-bottom: 8px;">
                        <div>
                            <label style="display: block; font-weight: bold; margin-bottom: 4px;">Username</label>
                            <input type="text" name="dest_sftp_username" value="{dest_sftp_username}"
                                   placeholder="sftp_user"
                                   style="width: 100%; padding: 6px; border: 1px solid #cbd5e1; border-radius: 3px;">
                        </div>
                        <div>
                            <label style="display: block; font-weight: bold; margin-bottom: 4px;">Password</label>
                            <input type="password" name="dest_sftp_password" value="{dest_sftp_password}"
                                   placeholder="leave blank if using a private key"
                                   style="width: 100%; padding: 6px; border: 1px solid #cbd5e1; border-radius: 3px;">
                        </div>
                    </div>
                    <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 12px; margin-bottom: 8px;">
                        <div>
                            <label style="display: block; font-weight: bold; margin-bottom: 4px;">Private Key Path</label>
                            <input type="text" name="dest_sftp_key_path" value="{dest_sftp_key_path}"
                                   placeholder="/path/to/id_rsa"
                                   style="width: 100%; padding: 6px; border: 1px solid #cbd5e1; border-radius: 3px;">
                        </div>
                        <div>
                            <label style="display: block; font-weight: bold; margin-bottom: 4px;">Key Passphrase</label>
                            <input type="password" name="dest_sftp_key_pass" value="{dest_sftp_key_pass}"
                                   placeholder="optional"
                                   style="width: 100%; padding: 6px; border: 1px solid #cbd5e1; border-radius: 3px;">
                        </div>
                    </div>
                    <div style="display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 12px; margin-bottom: 8px;">
                        <div>
                            <label style="display: block; font-weight: bold; margin-bottom: 4px;">Remote Directory</label>
                            <input type="text" name="dest_sftp_remote_dir" value="{dest_sftp_remote_dir}"
                                   placeholder="/uploads"
                                   style="width: 100%; padding: 6px; border: 1px solid #cbd5e1; border-radius: 3px;">
                        </div>
                        <div>
                            <label style="display: block; font-weight: bold; margin-bottom: 4px;">Filename Field</label>
                            <input type="text" name="dest_sftp_filename_field" value="{dest_sftp_filename_field}"
                                   placeholder="filename"
                                   style="width: 100%; padding: 6px; border: 1px solid #cbd5e1; border-radius: 3px;">
                        </div>
                        <div>
                            <label style="display: block; font-weight: bold; margin-bottom: 4px;">Content Field</label>
                            <input type="text" name="dest_sftp_content_field" value="{dest_sftp_content_field}"
                                   placeholder="content"
                                   style="width: 100%; padding: 6px; border: 1px solid #cbd5e1; border-radius: 3px;">
                        </div>
                    </div>
                    <small style="color: var(--text-muted); display:block; margin-top:4px;">Each message is uploaded as a file. If the payload has a <code>filename</code> key (or the configured filename field), it's used as the remote filename; otherwise a generated name is used. Dict payloads are serialized to JSON unless a content field is specified.</small>
                </div>
            </div>
        </div>

        <div class="section-box" style="margin-bottom: 12px;">
            <h2>Mapping Rules (JSON Array)</h2>
            <div class="section-body">
                <textarea name="mapping_rules" rows="6"
                          placeholder='[\\n  {{"source": "order_id", "target": "accession_num", "required": true}},\\n  {{"source": "lookups.patient.last_name", "target": "patient_last_name", "fn": "Uppercase"}}\\n]'
                          style="width: 100%; padding: 6px; border: 1px solid #cbd5e1; border-radius: 3px; font-family: var(--code-font); font-size: 11px; white-space: pre;">{mapping_rules_json}</textarea>
                <small style="color: var(--text-muted);">source, target, required, and optional fn (Uppercase, Lowercase, Trim Whitespace, Format, Default).</small>
            </div>
        </div>
        </div><!-- /ctab-panel-destination -->

        <div style="display: flex; justify-content: flex-end; gap: 8px; margin-top: 12px;">
            <button type="button" class="btn" onclick="switchTab('dashboard')">Cancel</button>
            <button type="submit" class="btn btn-primary">Save Channel</button>
        </div>
    </form>
    <script>
        function switchChannelTab(tabId) {{
            var ids = ['basic', 'source', 'destination'];
            for (var i = 0; i < ids.length; i++) {{
                var id = ids[i];
                document.getElementById('ctab-btn-' + id).classList.toggle('active', id === tabId);
                document.getElementById('ctab-panel-' + id).style.display = (id === tabId) ? 'block' : 'none';
            }}
        }}
        function toggleDestFields() {{
            var protocol = document.getElementById("dest_protocol").value;
            document.getElementById("dest_http_fields").style.display = (protocol === "http") ? "block" : "none";
            document.getElementById("dest_mllp_fields").style.display = (protocol === "mllp") ? "block" : "none";
            document.getElementById("dest_sftp_fields").style.display = (protocol === "sftp") ? "block" : "none";
        }}
        function toggleIngestFields() {{
            var itype = document.getElementById("ingestion_type").value;
            document.getElementById("ing_common_fields").style.display = (itype === "none") ? "none" : "block";
            document.getElementById("ing_poller_fields").style.display = (itype === "http_poller") ? "block" : "none";
            document.getElementById("ing_webhook_fields").style.display = (itype === "http_webhook") ? "block" : "none";
            document.getElementById("ing_mllp_fields").style.display = (itype === "mllp_server") ? "block" : "none";
            document.getElementById("ing_fw_fields").style.display = (itype === "file_watcher") ? "block" : "none";
            document.getElementById("ing_db_fields").style.display = (itype === "db_poller") ? "block" : "none";
        }}
    </script>
    """


@app.route("/ui/channels/save", methods=["POST"])
def save_channel():
    is_edit = request.form.get("is_edit") == "true"
    channel_id = request.form.get("channel_id", "").strip()
    name = request.form.get("name", "").strip()
    type_str = request.form.get("type", "").strip()
    status = request.form.get("status", "running")

    if not channel_id:
        return '<div style="color: var(--status-red); font-weight: bold; margin-bottom: 12px;">Channel ID is required.</div>', 400
    if not is_edit and not CHANNEL_ID_RE.match(channel_id):
        return '<div style="color: var(--status-red); font-weight: bold; margin-bottom: 12px;">Channel ID may only contain letters, numbers, underscores, and hyphens.</div>', 400
    if not name:
        return '<div style="color: var(--status-red); font-weight: bold; margin-bottom: 12px;">Channel Name is required.</div>', 400

    try:
        max_retries = int(request.form.get("max_retries", 3))
        base_backoff = int(request.form.get("base_backoff", 2))
    except ValueError:
        max_retries = 3
        base_backoff = 2
    retry_policy = {"max_retries": max_retries, "base_backoff_seconds": base_backoff}

    try:
        concurrency = max(1, min(16, int(request.form.get("concurrency", 1))))
    except ValueError:
        concurrency = 1

    mapping_rules_str = request.form.get("mapping_rules", "[]").strip()
    try:
        mapping_rules = json.loads(mapping_rules_str) if mapping_rules_str else []
        if not isinstance(mapping_rules, list):
            return '<div style="color: var(--status-red); font-weight: bold; margin-bottom: 12px;">Mapping rules must be a JSON array.</div>', 400
    except Exception as e:
        return f'<div style="color: var(--status-red); font-weight: bold; margin-bottom: 12px;">Invalid Mapping Rules JSON: {_esc(str(e))}</div>', 400

    enrichment_str = request.form.get("enrichment_config", "").strip()
    enrichment_config = None
    if enrichment_str:
        try:
            enrichment_config = json.loads(enrichment_str)
            required_keys = {"source_key_field", "target_table", "target_key_col", "fields"}
            if not required_keys.issubset(enrichment_config.keys()):
                missing = required_keys - enrichment_config.keys()
                return f'<div style="color: var(--status-red); font-weight: bold; margin-bottom: 12px;">Enrichment config missing keys: {", ".join(missing)}</div>', 400
        except Exception as e:
            return f'<div style="color: var(--status-red); font-weight: bold; margin-bottom: 12px;">Invalid Enrichment Config JSON: {_esc(str(e))}</div>', 400

    dest_protocol = request.form.get("dest_protocol", "none")
    destination = None
    if dest_protocol == "http":
        destination = {
            "type": "http",
            "endpoint_url": request.form.get("dest_http_url", "").strip(),
            "method": request.form.get("dest_http_method", "POST"),
            "auth_profile_id": request.form.get("dest_http_auth") or None,
        }
    elif dest_protocol == "mllp":
        try:
            port = int(request.form.get("dest_mllp_port", 0))
        except ValueError:
            port = 0
        destination = {
            "type": "mllp",
            "host": request.form.get("dest_mllp_host", "").strip(),
            "port": port,
        }
    elif dest_protocol == "sftp":
        try:
            sftp_port = int(request.form.get("dest_sftp_port", 22))
        except ValueError:
            sftp_port = 22
        destination = {
            "type": "sftp",
            "host": request.form.get("dest_sftp_host", "").strip(),
            "port": sftp_port,
            "username": request.form.get("dest_sftp_username", "").strip(),
            "password": request.form.get("dest_sftp_password", "").strip() or None,
            "private_key_path": request.form.get("dest_sftp_key_path", "").strip() or None,
            "private_key_passphrase": request.form.get("dest_sftp_key_pass", "").strip() or None,
            "remote_dir": request.form.get("dest_sftp_remote_dir", ".").strip() or ".",
            "filename_field": request.form.get("dest_sftp_filename_field", "").strip() or None,
            "content_field": request.form.get("dest_sftp_content_field", "").strip() or None,
        }

    ingestion_type = request.form.get("ingestion_type", "none")
    ingestion_config = None

    ing_max_queue_depth = request.form.get("ing_max_queue_depth", "").strip()
    common_ing_fields = {}
    if ing_max_queue_depth:
        try:
            common_ing_fields["max_queue_depth"] = int(ing_max_queue_depth)
        except ValueError:
            pass
    ing_idempotency_field = request.form.get("ing_idempotency_field", "").strip()
    if ing_idempotency_field:
        common_ing_fields["idempotency_key_field"] = ing_idempotency_field

    if ingestion_type == "http_poller":
        try:
            interval_s = int(request.form.get("ing_interval", 10))
        except ValueError:
            interval_s = 10
        if interval_s < 5:
            return '<div style="color: var(--status-red); font-weight: bold; margin-bottom: 12px;">Poll interval must be at least 5 seconds.</div>', 400
        ingestion_config = {
            "url": request.form.get("ing_url", "").strip(),
            "interval_s": interval_s,
            "records_path": request.form.get("ing_records_path", "").strip(),
            "cursor_param": request.form.get("ing_cursor_param", "").strip() or None,
            "cursor_field": request.form.get("ing_cursor_field", "").strip() or None,
            "auth_profile_id": request.form.get("ing_auth_profile") or None,
            **common_ing_fields,
        }
    elif ingestion_type == "http_webhook":
        ingestion_config = {
            "shared_secret": request.form.get("ing_secret", "").strip() or None,
            "sig_header": request.form.get("ing_sig_header", "X-Signature").strip() or "X-Signature",
            **common_ing_fields,
        }
    elif ingestion_type == "mllp_server":
        try:
            mllp_port = int(request.form.get("ing_mllp_port", 0))
        except ValueError:
            mllp_port = 0
        if not mllp_port:
            return '<div style="color: var(--status-red); font-weight: bold; margin-bottom: 12px;">MLLP Server requires a port.</div>', 400
        try:
            max_conn = int(request.form.get("ing_mllp_max_conn", 20))
        except ValueError:
            max_conn = 20
        try:
            idle_timeout = int(request.form.get("ing_mllp_idle_timeout", 300))
        except ValueError:
            idle_timeout = 300
        ingestion_config = {
            "host": request.form.get("ing_mllp_host", "0.0.0.0").strip() or "0.0.0.0",
            "port": mllp_port,
            "max_connections": max_conn,
            "idle_timeout_s": idle_timeout,
            **common_ing_fields,
        }
    elif ingestion_type == "file_watcher":
        directory = request.form.get("ing_fw_directory", "").strip()
        if not directory:
            return '<div style="color: var(--status-red); font-weight: bold; margin-bottom: 12px;">File Watcher requires a directory.</div>', 400
        try:
            fw_interval = int(request.form.get("ing_fw_interval", 5))
        except ValueError:
            fw_interval = 5
        extensions_raw = request.form.get("ing_fw_extensions", ".csv,.hl7,.txt")
        extensions = [e.strip() for e in extensions_raw.split(",") if e.strip()]
        ingestion_config = {
            "directory": directory,
            "interval_s": fw_interval,
            "extensions": extensions,
            **common_ing_fields,
        }
    elif ingestion_type == "db_poller":
        connection_string = request.form.get("ing_db_conn", "").strip()
        query = request.form.get("ing_db_query", "").strip()
        if not connection_string:
            return '<div style="color: var(--status-red); font-weight: bold; margin-bottom: 12px;">DB Poller requires a connection string.</div>', 400
        if not query:
            return '<div style="color: var(--status-red); font-weight: bold; margin-bottom: 12px;">DB Poller requires a query.</div>', 400
        try:
            db_interval = int(request.form.get("ing_db_interval", 10))
        except ValueError:
            db_interval = 10
        if db_interval < 5:
            return '<div style="color: var(--status-red); font-weight: bold; margin-bottom: 12px;">DB Poll interval must be at least 5 seconds.</div>', 400
        ingestion_config = {
            "connection_string": connection_string,
            "query": query,
            "db_type": request.form.get("ing_db_type", "sqlite"),
            "interval_s": db_interval,
            "cursor_field": request.form.get("ing_db_cursor_field", "").strip() or None,
            "cursor_param": request.form.get("ing_db_cursor_param", "").strip() or None,
            **common_ing_fields,
        }
    else:
        ingestion_type = None

    try:
        with queue._get_conn() as conn:
            if is_edit:
                conn.execute("""
                    UPDATE channels
                    SET name = ?, type = ?, status = ?, retry_policy = ?, mapping_rules = ?, destination = ?,
                        ingestion_type = ?, ingestion_config = ?, enrichment_config = ?, concurrency = ?
                    WHERE channel_id = ?
                """, (
                    name, type_str, status,
                    json.dumps(retry_policy),
                    json.dumps(mapping_rules),
                    json.dumps(destination) if destination else None,
                    ingestion_type,
                    json.dumps(ingestion_config) if ingestion_config else None,
                    json.dumps(enrichment_config) if enrichment_config else None,
                    concurrency,
                    channel_id,
                ))
            else:
                conn.execute("""
                    INSERT INTO channels
                        (channel_id, name, enabled, status, type, retry_policy, mapping_rules, destination,
                         ingestion_type, ingestion_config, enrichment_config, concurrency)
                    VALUES (?, ?, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    channel_id, name, status, type_str,
                    json.dumps(retry_policy),
                    json.dumps(mapping_rules),
                    json.dumps(destination) if destination else None,
                    ingestion_type,
                    json.dumps(ingestion_config) if ingestion_config else None,
                    json.dumps(enrichment_config) if enrichment_config else None,
                    concurrency,
                ))
            conn.commit()
    except sqlite3.IntegrityError:
        return '<div style="color: var(--status-red); font-weight: bold; margin-bottom: 12px;">Channel ID already exists.</div>', 400
    except Exception as e:
        return f'<div style="color: var(--status-red); font-weight: bold; margin-bottom: 12px;">Database Error: {_esc(str(e))}</div>', 500

    registry.load_all_configs()
    sync_webhooks()
    # The form targets #form-flash (see bug fix above), so the refreshed
    # channel table is pushed separately via an htmx out-of-band swap
    # rather than as the main response body.
    return f'<div hx-swap-oob="innerHTML:#channel-table-body">{get_ui_channels()}</div>'


@app.route("/ui/channels/<channel_id>", methods=["DELETE"])
def delete_channel(channel_id):
    try:
        with queue._get_conn() as conn:
            conn.execute("DELETE FROM channels WHERE channel_id = ?", (channel_id,))
            conn.commit()
    except Exception as e:
        return f'<div style="color: var(--status-red); font-weight: bold; margin-bottom: 12px;">Database Error: {_esc(str(e))}</div>', 500

    registry.load_all_configs()
    sync_webhooks()
    return get_ui_channels()


@app.route("/channels/<channel_id>/messages")
def channel_messages_page(channel_id):
    """Full page (deep-linkable) view for a single channel's messages, built
    around a Mirth-Connect-inspired split Source|Destination inspector."""
    configs = registry.load_all_configs()
    info = configs.get(channel_id)
    if not info:
        return app.response_class(
            status=404,
            response='<h1 style="padding:20px;">Channel not found</h1>'
                     '<p style="padding:0 20px;"><a href="/">Back to Dashboard</a></p>',
        )
    return render_template(
        "channel_messages.html",
        channel_id=channel_id,
        channel_name=info.get("name") or channel_id,
        channel_status=info.get("status") or "paused",
    )


@app.route("/channels/<channel_id>/messages/body")
def get_channel_messages(channel_id):
    """htmx fragment: the <tbody> rows of a channel's recent-message list
    (state, trace id, attempts, received time, payload preview, error)."""
    with queue._get_conn() as conn:
        rows = conn.execute(
            """
            SELECT trace_id, state, attempts, raw_payload, last_error, created_at
            FROM queue
            WHERE channel_id = ?
            ORDER BY created_at DESC
            LIMIT 100
            """,
            (channel_id,),
        ).fetchall()

    if not rows:
        return ('<tr><td colspan="6" style="text-align:center; color: var(--text-muted); padding: 12px;">'
                'No messages yet for this channel.</td></tr>')

    safe_cid = _esc(channel_id, quote=True)
    rows_html = ""
    for r in rows:
        state = (r["state"] or "UNKNOWN").upper()
        pill_class, pill_label = _MESSAGE_STATE_PILL.get(state, ("pill-paused", state))
        safe_trace_id = _esc(r["trace_id"], quote=True)
        safe_trace_short = _esc(r["trace_id"][:12])
        safe_pill = _esc(pill_label)
        safe_attempts = _esc(str(r["attempts"] or 0))
        safe_received = _esc(_ago(r["created_at"]))
        safe_preview = _esc(_payload_preview(r["raw_payload"]))
        safe_err = _esc((r["last_error"] or "")[:80])
        rows_html += f"""<tr data-trace-id="{safe_trace_id}"
                hx-get="/channels/{safe_cid}/messages/{safe_trace_id}"
                hx-target="#split-inspector" hx-swap="innerHTML"
                onclick="selectMessageRow(this)">
            <td><span class="pill {pill_class}">{safe_pill}</span></td>
            <td><code style="font-size:10px;" title="{safe_trace_id}">{safe_trace_short}...</code></td>
            <td>{safe_attempts}</td>
            <td>{safe_received}</td>
            <td><code style="font-size:10px; color:#475569; display:block; max-width:280px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap;">{safe_preview}</code></td>
            <td style="color: var(--status-red); font-size:11px; max-width: 200px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap;">{safe_err}</td>
        </tr>"""
    return rows_html


@app.route("/channels/<channel_id>/messages/<trace_id>")
def get_channel_message_detail(channel_id, trace_id):
    """htmx fragment: the split inspector for one message. Source pane shows
    the raw inbound payload; Destination pane shows the transformed outbound
    payload (or a failure notice), plus error details/stack trace when present."""
    with queue._get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM queue WHERE trace_id = ? AND channel_id = ?",
            (trace_id, channel_id),
        ).fetchone()

    if not row:
        return '<div class="code-block">Message not found for this channel.</div>'

    state = (row["state"] or "UNKNOWN").upper()
    pill_class, pill_label = _MESSAGE_STATE_PILL.get(state, ("pill-paused", state))

    # Every dynamic value below originates in inbound payloads, error strings,
    # or user-supplied data, so each is HTML-escaped before raw splicing.
    safe_trace = _esc(row["trace_id"], quote=True)
    safe_pill = _esc(pill_label)
    safe_attempts = _esc(str(row["attempts"] or 0))
    safe_created = _esc(_ago(row["created_at"]))
    safe_idem = _esc(row["idempotency_key"] or "—")

    out = [f"""<div class="msg-meta">
            <code style="font-size:11px;">{safe_trace}</code>
            <span class="pill {pill_class}">{safe_pill}</span>
            <span>attempts: <b>{safe_attempts}</b></span>
            <span>received: {safe_created}</span>
            <span>idempotency key: <code style="font-size:10px;">{safe_idem}</code></span>
        </div>"""]

    if row["last_error"]:
        safe_err = _esc(row["last_error"])
        banner = f'<div class="error-banner"><b>Last error:</b> {safe_err}</div>'
        if row["last_traceback"]:
            safe_traceback = _esc(row["last_traceback"])
            banner += (f'<details class="trace-details"><summary>Stack trace</summary>'
                       f'<pre class="trace-block">{safe_traceback}</pre></details>')
        out.append(banner)
    else:
        out.append('<div class="error-banner banner-ok"><b>No processing errors.</b></div>')

    source_pane = f'<div class="code-block">{_esc(_pretty_payload(row["raw_payload"]))}</div>'

    transformed_pretty = _pretty_payload(row["transformed_payload"])
    if transformed_pretty is not None:
        dest_pane = f'<div class="code-block dest-sent">{_esc(transformed_pretty)}</div>'
    else:
        dest_pane = ('<div class="code-block dim-block">No outbound payload — '
                     'processing failed before reaching the destination.</div>')

    out.append(f"""<div class="grid-2col pane-grid">
        <div class="section-box pane">
            <div class="section-header-row pane-header">
                <span class="pane-title">Source</span>
                <span class="pane-sub">raw inbound payload</span>
            </div>
            <div style="padding: 12px;">{source_pane}</div>
        </div>
        <div class="section-box pane">
            <div class="section-header-row pane-header">
                <span class="pane-title">Destination</span>
                <span class="pane-sub">transformed outbound payload</span>
            </div>
            <div style="padding: 12px;">{dest_pane}</div>
        </div>
    </div>""")

    return "".join(out)


@app.route("/ui/dlq")
def get_ui_dlq():
    with queue._get_conn() as conn:
        rows = conn.execute(
            "SELECT trace_id, channel_id, created_at, last_error FROM queue WHERE UPPER(state) = 'DEAD_LETTER'"
        ).fetchall()

    if not rows:
        return '<tr><td colspan="4" style="text-align:center; color: var(--text-muted); padding: 12px;">No DLQ records found.</td></tr>'

    rows_html = ""
    for r in rows:
        # trace_id and last_error can contain attacker-controlled bytes from
        # inbound payloads (e.g. a channel name or error string with a
        # <script> tag), so escape before splicing into raw HTML.
        safe_trace_id = _esc(r["trace_id"], quote=True)
        safe_trace_id_short = _esc(r["trace_id"][:12])
        safe_last_error = _esc(r["last_error"] or "")
        last_seen = _ago(r["created_at"])
        rows_html += f"""
        <tr>
            <td><code style="font-size: 10px;" title="{safe_trace_id}">{safe_trace_id_short}...</code></td>
            <td>{last_seen}</td>
            <td style="color: var(--status-red);">{safe_last_error}</td>
            <td>
                <div style="display:flex; gap:4px;">
                    <button class="btn" hx-get="/ui/dlq/{safe_trace_id}" hx-target="#inspector-box">Inspect</button>
                    <button class="btn btn-primary" hx-post="/ui/dlq/{safe_trace_id}/requeue" hx-target="#dlq-table-body" hx-swap="innerHTML">Requeue</button>
                    <button class="btn btn-danger" hx-delete="/ui/dlq/{safe_trace_id}" hx-confirm="Discard this DLQ entry permanently?" hx-target="#dlq-table-body" hx-swap="innerHTML">Discard</button>
                </div>
            </td>
        </tr>
        """
    return rows_html


@app.route("/ui/dlq/<trace_id>")
def get_dlq_detail(trace_id):
    with queue._get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM queue WHERE trace_id = ?", (trace_id,)
        ).fetchone()

    if not row:
        return '<div class="code-block">Record not found</div>'

    raw = row["raw_payload"]
    try:
        if raw.startswith(("{", "[")):
            raw = json.loads(raw)
    except Exception:
        pass

    data = {
        "trace_id": row["trace_id"],
        "channel_id": row["channel_id"],
        "state": row["state"],
        "attempts": row["attempts"],
        "raw_payload": raw,
        "last_error": row["last_error"],
    }

    # data embeds the original inbound raw_payload verbatim, which is
    # attacker-controlled, so escape the rendered JSON before it goes into
    # HTML. No id here — this fragment is swapped *into* #inspector-box
    # (defined once in index.html); repeating the id there created a
    # duplicate that only "worked" because querySelector picks the first match.
    return f'<div class="code-block">{_esc(json.dumps(data, indent=2))}</div>'


@app.route("/ui/audit")
def get_audit_ui():
    """Message audit trail lookup by trace_id — unlike /ui/dlq/<trace_id>,
    this works for any message regardless of current state (queued,
    delivered, retried, dead-lettered) and shows the full event history, not
    just a snapshot of current row state."""
    trace_id = request.args.get("trace_id", "").strip()
    if not trace_id:
        return '<div class="code-block">Enter a trace_id above and click Trace.</div>'

    trail = queue.get_audit_trail(trace_id)
    if not trail:
        # trace_id comes straight from the query string — a classic
        # reflected-XSS vector — so it must be escaped before it's echoed
        # back into the page.
        return f'<div class="code-block">No audit history found for trace_id: {_esc(trace_id)}</div>'

    with queue._get_conn() as conn:
        row = conn.execute("SELECT channel_id, state FROM queue WHERE trace_id = ?", (trace_id,)).fetchone()
    current = {"channel_id": row["channel_id"], "current_state": row["state"]} if row else {"note": "no longer in active queue table (discarded from DLQ)"}

    data = {"trace_id": trace_id, **current, "history": trail}
    # history events and trace_id are both attacker-reachable (trace_id via
    # the query string, history payloads via inbound messages), so escape
    # the rendered JSON. No id on this fragment for the same reason as
    # get_dlq_detail above — it's swapped into the single #inspector-box.
    return f'<div class="code-block">{_esc(json.dumps(data, indent=2))}</div>'


@app.route("/api/audit/<trace_id>")
def get_audit_api(trace_id):
    """JSON equivalent of /ui/audit — 'what did we receive/send for trace X
    at 14:03', answerable via API instead of the dashboard."""
    trail = queue.get_audit_trail(trace_id)
    if not trail:
        return jsonify({"error": "trace_id not found"}), 404
    with queue._get_conn() as conn:
        row = conn.execute("SELECT channel_id, state FROM queue WHERE trace_id = ?", (trace_id,)).fetchone()
    current = {"channel_id": row["channel_id"], "current_state": row["state"]} if row else {}
    return jsonify({"trace_id": trace_id, **current, "history": trail})


@app.route("/ui/dlq/clear", methods=["POST"])
def clear_dlq():
    with queue._get_conn() as conn:
        conn.execute("DELETE FROM queue WHERE UPPER(state) = 'DEAD_LETTER'")
        conn.commit()
    return get_ui_dlq()


@app.route("/ui/dlq/requeue", methods=["POST"])
def requeue_dlq():
    """Requeue-all button — matches the 'Re-queue All' action from the blueprint."""
    with queue._get_conn() as conn:
        conn.execute(
            "UPDATE queue SET state = 'QUEUED', attempts = 0, last_error = NULL, next_retry_at = NULL WHERE UPPER(state) = 'DEAD_LETTER'"
        )
        conn.commit()
    return get_ui_dlq()


@app.route("/ui/dlq/<trace_id>/requeue", methods=["POST"])
def requeue_dlq_entry(trace_id):
    """Requeue a single DLQ entry — resets attempts so it gets the full
    retry budget again rather than immediately re-dying on attempt N."""
    with queue._get_conn() as conn:
        conn.execute(
            "UPDATE queue SET state = 'QUEUED', attempts = 0, last_error = NULL, next_retry_at = NULL WHERE trace_id = ? AND UPPER(state) = 'DEAD_LETTER'",
            (trace_id,),
        )
        conn.commit()
    return get_ui_dlq()


@app.route("/ui/dlq/<trace_id>", methods=["DELETE"])
def discard_dlq_entry(trace_id):
    with queue._get_conn() as conn:
        conn.execute("DELETE FROM queue WHERE trace_id = ? AND UPPER(state) = 'DEAD_LETTER'", (trace_id,))
        conn.commit()
    return get_ui_dlq()


@app.route("/api/ingest/<channel_id>", methods=["POST"])
def ingest_message(channel_id):
    """Manual/generic inbound endpoint — always available regardless of a
    channel's configured ingestion_type, useful for testing or for source
    systems that just POST directly instead of going through a poller/webhook.
    Honors the same max_queue_depth / idempotency_key_field settings as the
    channel's configured ingestion, if any, so behavior stays consistent
    whichever path a message comes in through."""
    data = request.get_json(silent=True) or request.form.to_dict()
    if not data:
        return jsonify({"error": "Empty or non-JSON payload"}), 400

    conf = registry.load_config(channel_id) or {}
    icfg = conf.get("ingestion_config") or {}

    max_depth = icfg.get("max_queue_depth")
    if max_depth is not None and queue.queue_depth(channel_id) >= max_depth:
        return jsonify({"status": "rejected", "reason": "queue at capacity"}), 503

    env = Envelope(channel_id=channel_id, raw_payload=data)
    key_field = icfg.get("idempotency_key_field")
    if key_field and isinstance(data, dict):
        env.idempotency_key = str(data.get(key_field, "")) or None

    accepted = queue.enqueue(env)
    if not accepted:
        return jsonify({"status": "duplicate_ignored", "trace_id": env.trace_id}), 200
    return jsonify({"status": "queued", "trace_id": env.trace_id}), 201


@app.route("/ui/test/simulate", methods=["POST"])
def simulate_test_messages():
    queue.enqueue(
        Envelope(
            channel_id="his_to_lis",
            raw_payload={"order_id": 9001, "doctor_username": "dr_smith", "test": "COMPREHENSIVE_METABOLIC"},
        )
    )

    queue.enqueue(
        Envelope(
            channel_id="his_to_lis", raw_payload={"test": "LIPID_PANEL"}
        )
    )

    return get_ui_metrics()


if __name__ == "__main__":
    app.run(port=5000, debug=True)