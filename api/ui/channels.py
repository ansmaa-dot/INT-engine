"""Channel CRUD blueprint — metrics, channel list, toggle, form, save, delete."""

import json

from flask import Blueprint, render_template, render_template_string, request

from api.deps import registry, queue, sync_webhooks, CHANNEL_ID_RE
from api.ui.helpers import _esc, ago, channel_health
from engine.config_loader import ConfigValidationError, DESTINATIONS, TRANSPORTS

bp = Blueprint("ui_channels", __name__)


# ---------------------------------------------------------------------------
# Metrics
# ---------------------------------------------------------------------------

@bp.route("/ui/metrics")
def get_ui_metrics():
    configs = registry.load_all_configs()
    health = channel_health(configs, queue)

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


# ---------------------------------------------------------------------------
# Channel list (HTMX fragment)
# ---------------------------------------------------------------------------

@bp.route("/ui/channels")
def get_ui_channels():
    configs = registry.load_all_configs()
    health = channel_health(configs, queue)
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

        inbound_transport = info.get("inbound_transport") or "manual"

        if h["last_error_at"] and (not h["last_delivered_at"] or h["last_error_at"] > h["last_delivered_at"]):
            last_line = f'<span style="color: var(--status-red);">last error {ago(h["last_error_at"])}</span>'
        elif h["last_delivered_at"]:
            last_line = f'delivered {ago(h["last_delivered_at"])} \u00b7 {h["dnpm"]:.1f}/min'
        else:
            last_line = "no activity yet"

        safe_cid = _esc(cid, quote=True)
        safe_name = _esc(info.get("name") or cid)
        safe_type = _esc(info.get("destination") or "none")
        safe_ingestion_type = _esc(inbound_transport)

        rows_html += f"""
        <tr>
            <td style="width:50%;">
                <a class="entry-link" href="/channels/{safe_cid}/messages" title="Open channel messages"><strong>{safe_name}</strong></a> <span class="pill {load_class[h['load_state']]}">{load_label[h['load_state']]}</span><br>
                <small style="color: #64748b;">in: {safe_ingestion_type} \u00b7 out: {safe_type}</small><br>
                <small>queue: <b style="color:{depth_color[h['load_state']]};">{h['depth']}</b> \u00b7 retries: {h['retries']} \u00b7 dlq: {h['dead_letter']} \u00b7 in-flight: {h['in_flight']}</small><br>
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


# ---------------------------------------------------------------------------
# Toggle channel
# ---------------------------------------------------------------------------

@bp.route("/ui/channels/<channel_id>/<action>", methods=["POST"])
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


# ---------------------------------------------------------------------------
# Channel form (new / edit)
# ---------------------------------------------------------------------------

@bp.route("/ui/channels/new")
def new_channel_form():
    return _render_channel_form()


@bp.route("/ui/channels/<channel_id>/edit")
def edit_channel_form(channel_id):
    configs = registry.load_all_configs()
    config = configs.get(channel_id)
    if not config:
        return '<div style="color: var(--status-red); font-weight: bold;">Channel not found</div>', 404
    return _render_channel_form(config)


@bp.route("/ui/channels/<channel_id>", methods=["DELETE"])
def delete_channel(channel_id):
    try:
        with queue._get_conn() as conn:
            cur = conn.execute("DELETE FROM channels WHERE channel_id = ?", (channel_id,))
            conn.commit()
            if cur.rowcount == 0:
                return '<tr><td colspan="3" style="color: var(--status-red); padding: 12px;">Channel not found.</td></tr>', 404
        registry.load_all_configs()
        sync_webhooks()
    except Exception as e:
        print(f"[Error] Failed to delete channel: {e}")
    return get_ui_channels()


# ---------------------------------------------------------------------------
# Transport / Destination field definitions
# ---------------------------------------------------------------------------

_TRANSPORT_FIELDS = {
    "http_webhook": [
        ("shared_secret", "Shared Secret", "text", "", "optional HMAC secret", False),
        ("sig_header", "Signature Header", "text", "X-Signature", "", False),
        ("max_queue_depth", "Max Queue Depth", "number", "", "0 = unlimited", False),
        ("idempotency_key_field", "Idempotency Key Field", "text", "", "JSON field name for dedup", False),
    ],
    "http_poller": [
        ("url", "Poll URL *", "text", "", "https://...", True),
        ("interval_s", "Interval (s)", "number", "10", "", False),
        ("auth_profile_id", "Auth Profile ID", "text", "", "from config/auth_profiles.json", False),
        ("max_queue_depth", "Max Queue Depth", "number", "", "0 = unlimited", False),
        ("idempotency_key_field", "Idempotency Key Field", "text", "", "JSON field name for dedup", False),
    ],
    "mllp": [
        ("host", "Listen Host", "text", "0.0.0.0", "", False),
        ("port", "Listen Port *", "number", "", "e.g. 2575", True),
        ("max_connections", "Max Connections", "number", "20", "", False),
        ("idle_timeout_s", "Idle Timeout (s)", "number", "300", "", False),
        ("max_queue_depth", "Max Queue Depth", "number", "", "0 = unlimited", False),
        ("idempotency_from_msh10", "Idempotency from MSH-10", "checkbox", "", "", False),
    ],
    "file_watcher": [
        ("directory", "Watch Directory *", "text", "", "/path/to/incoming", True),
        ("interval_s", "Poll Interval (s)", "number", "5", "", False),
        ("extensions", "File Extensions", "text", ".csv,.hl7,.txt", "comma-separated", False),
        ("max_queue_depth", "Max Queue Depth", "number", "", "0 = unlimited", False),
    ],
    "db_poller": [
        ("connection_string", "Connection String *", "text", "", "sqlite:///data.db", True),
        ("query", "SQL Query *", "textarea", "", "SELECT * FROM ...", True),
        ("db_type", "DB Type", "select:sqlite,postgresql,mysql", "sqlite", "", False),
        ("interval_s", "Poll Interval (s)", "number", "10", "", False),
        ("cursor_field", "Cursor Field", "text", "", "e.g. updated_at", False),
        ("cursor_param", "Cursor Param", "text", "", "e.g. :last_cursor", False),
        ("max_queue_depth", "Max Queue Depth", "number", "", "0 = unlimited", False),
        ("idempotency_key_field", "Idempotency Key Field", "text", "", "column name for dedup", False),
    ],
}

_DESTINATION_FIELDS = {
    "http": [
        ("endpoint_url", "Endpoint URL *", "text", "", "https://...", True),
        ("method", "HTTP Method", "select:POST,PUT,PATCH", "POST", "", False),
        ("auth_profile_id", "Auth Profile ID", "text", "", "from config/auth_profiles.json", False),
        ("headers", "Headers (JSON)", "textarea", "", '{"Authorization":"Bearer ..."}', False),
        ("timeout_s", "Timeout (s)", "number", "5", "", False),
    ],
    "mllp": [
        ("host", "MLLP Host *", "text", "", "e.g. 10.0.0.5", True),
        ("port", "MLLP Port *", "number", "", "e.g. 2575", True),
    ],
    "sftp": [
        ("host", "SFTP Host *", "text", "", "sftp.example.com", True),
        ("port", "Port", "number", "22", "", False),
        ("username", "Username", "text", "", "", False),
        ("password", "Password", "password", "", "", False),
        ("private_key_path", "Private Key Path", "text", "", "~/.ssh/id_rsa", False),
        ("private_key_passphrase", "Key Passphrase", "password", "", "", False),
        ("remote_dir", "Remote Dir", "text", ".", "", False),
        ("timeout_s", "Timeout (s)", "number", "10", "", False),
    ],
}

_TRANSPORT_LABELS = {
    "http_webhook": "HTTP Webhook", "http_poller": "HTTP Poller",
    "mllp": "MLLP Server", "file_watcher": "File Watcher", "db_poller": "DB Poller",
}
_DEST_LABELS = {"http": "HTTP Client", "mllp": "MLLP Client", "sftp": "SFTP Client"}


def _make_field_dict(name, label, ftype, default, placeholder, required, current_value=None):
    return {
        "name": name, "label": label, "ftype": ftype,
        "default": default, "placeholder": placeholder,
        "required": required,
        "value": current_value if current_value is not None else default,
    }


def _render_channel_form(config=None):
    """Build structured data and render the channel-form Jinja2 template."""
    from nodes.codec import keys as codec_keys
    from api.ui.helpers import codec_label

    if config is None:
        config = {
            "channel_id": "", "name": "", "enabled": True, "status": "running",
            "concurrency": 1,
            "inbound_transport": "http_webhook", "inbound_transport_config": {},
            "inbound_codec": "json", "outbound_codec": "json",
            "destination": "http", "destination_config": {"endpoint_url": ""},
            "mapping_id": "", "mapping_version": "", "enrichment_id": "",
            "enrichment_version": "", "retry_policy_id": "default", "semantics": None,
        }

    trans_cfg = config.get("inbound_transport_config") or {}
    dest_cfg = config.get("destination_config") or {}
    selected_transport = config.get("inbound_transport", "http_webhook")
    selected_dest = config.get("destination", "http")
    selected_codec = config.get("inbound_codec", "json")
    selected_out_codec = config.get("outbound_codec", "json")

    # --- Option lists ---
    transports = [{"value": t, "label": _TRANSPORT_LABELS.get(t, t),
                    "selected": t == selected_transport}
                  for t in sorted(_TRANSPORT_FIELDS)]

    codec_opts = sorted(codec_keys())
    codecs = [{"value": k, "label": codec_label(k), "selected": k == selected_codec}
              for k in codec_opts]
    outbound_codecs = [{"value": k, "label": codec_label(k), "selected": k == selected_out_codec}
                       for k in codec_opts]

    destinations = [{"value": d, "label": _DEST_LABELS.get(d, d),
                     "selected": d == selected_dest}
                    for d in sorted(_DESTINATION_FIELDS)]

    mappings = [{"value": m["mapping_id"], "label": f"{m['mapping_id']} v{m['version']}",
                 "selected": m["mapping_id"] == config.get("mapping_id")}
                for m in registry.list_mappings()]
    enrichments = [{"value": e["enrichment_id"], "label": f"{e['enrichment_id']} v{e['version']}",
                    "selected": e["enrichment_id"] == config.get("enrichment_id")}
                   for e in registry.list_enrichments()]
    retry_policies = [{"value": rp["retry_policy_id"], "label": rp["retry_policy_id"],
                       "selected": rp["retry_policy_id"] == config.get("retry_policy_id")}
                      for rp in registry.list_retry_policies()]

    # --- Transport groups ---
    transport_groups = []
    for transport, fields in _TRANSPORT_FIELDS.items():
        transport_groups.append({
            "transport": transport,
            "transport_label": _TRANSPORT_LABELS.get(transport, transport),
            "visible": transport == selected_transport,
            "fields": [_make_field_dict(name, label, ftype, default, placeholder,
                                        required, current_value=trans_cfg.get(name))
                       for name, label, ftype, default, placeholder, required in fields],
        })

    # --- Destination groups ---
    dest_groups = []
    for dest, fields in _DESTINATION_FIELDS.items():
        dest_groups.append({
            "destination": dest,
            "dest_label": _DEST_LABELS.get(dest, dest),
            "visible": dest == selected_dest,
            "fields": [_make_field_dict(name, label, ftype, default, placeholder,
                                        required, current_value=dest_cfg.get(name))
                       for name, label, ftype, default, placeholder, required in fields],
        })

    # --- Config for template ---
    tmpl_config = {
        "channel_id": config.get("channel_id", ""),
        "name": config.get("name", ""),
        "status": config.get("status", "running"),
        "concurrency": config.get("concurrency", 1),
        "enabled": config.get("enabled", True),
        "mapping_version": config.get("mapping_version") or "",
        "enrichment_version": config.get("enrichment_version") or "",
        "semantics": config.get("semantics"),
    }
    if isinstance(tmpl_config["semantics"], (dict, list)):
        tmpl_config["semantics"] = json.dumps(tmpl_config["semantics"], indent=2)

    return render_template_string(
        render_template("channel_form.html",
                        config=tmpl_config,
                        transports=transports,
                        codecs=codecs,
                        outbound_codecs=outbound_codecs,
                        destinations=destinations,
                        transport_groups=transport_groups,
                        dest_groups=dest_groups,
                        mappings=mappings,
                        enrichments=enrichments,
                        retry_policies=retry_policies,
                        ))


# ---------------------------------------------------------------------------
# Save channel
# ---------------------------------------------------------------------------

@bp.route("/ui/channels/save", methods=["POST"])
def save_channel():
    is_edit = request.form.get("is_edit") == "true"
    channel_id = request.form.get("channel_id", "").strip()
    if not is_edit and not CHANNEL_ID_RE.match(channel_id):
        return ('<div style="color: var(--status-red); font-weight: bold; margin-bottom: 12px;">'
                'Channel ID may only contain letters, numbers, underscores, and hyphens.</div>'), 400

    def _json_field(name, default=None):
        raw = request.form.get(name, "").strip()
        if not raw:
            return default
        try:
            val = json.loads(raw)
        except Exception as e:
            raise ConfigValidationError(f"Invalid JSON for {name}: {e}")
        return val

    def _int_field(name, default):
        try:
            return int(request.form.get(name) or default)
        except ValueError:
            return default

    try:
        definition = {
            "channel_id": channel_id,
            "name": request.form.get("name", "").strip(),
            "enabled": request.form.get("enabled") == "on",
            "status": request.form.get("status", "running"),
            "concurrency": max(1, min(16, _int_field("concurrency", 1))),
            "inbound_transport": request.form.get("inbound_transport", ""),
            "inbound_transport_config": _json_field("inbound_transport_config", {}) or {},
            "inbound_codec": request.form.get("inbound_codec", ""),
            "outbound_codec": request.form.get("outbound_codec", ""),
            "destination": request.form.get("destination", ""),
            "destination_config": _json_field("destination_config", {}) or {},
            "mapping_id": request.form.get("mapping_id", "") or None,
            "mapping_version": _int_field("mapping_version", 0) or None,
            "enrichment_id": request.form.get("enrichment_id", "") or None,
            "enrichment_version": _int_field("enrichment_version", 0) or None,
            "retry_policy_id": request.form.get("retry_policy_id", "") or None,
            "semantics": _json_field("semantics", None),
        }
        registry.save_channel_definition(definition)
    except ConfigValidationError as e:
        return f'<div style="color: var(--status-red); font-weight: bold; margin-bottom: 12px;">{_esc(str(e))}</div>', 400
    except Exception as e:
        return f'<div style="color: var(--status-red); font-weight: bold; margin-bottom: 12px;">Database Error: {_esc(str(e))}</div>', 500

    registry.load_all_configs()
    sync_webhooks()
    return (f'<div hx-swap-oob="innerHTML:#channel-table-body">{get_ui_channels()}</div>'
            f'<div style="padding:12px 0;">'
            f'<div style="color:var(--status-green);font-weight:bold;margin-bottom:8px;">&#10003; Channel <code>{_esc(channel_id)}</code> saved successfully.</div>'
            f'<button type="button" class="btn btn-primary" onclick="switchTab(&apos;dashboard&apos;)">&#8592; Back to Dashboard</button>'
            f'</div>')
