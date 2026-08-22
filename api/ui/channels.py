"""Channel CRUD blueprint — metrics, channel list, toggle, form, save, delete."""

import json

from flask import Blueprint, render_template, render_template_string, request

from api.deps import registry, queue, sync_webhooks, CHANNEL_ID_RE
from api.ui.helpers import _esc, ago, channel_health
from engine.config_loader import ConfigValidationError, DESTINATIONS, TRANSPORTS

# Only needed if you've adopted Flask-WTF CSRF protection. Remove this
# import and the validate_csrf() call in save_channel() if you haven't.
from flask_wtf.csrf import validate_csrf, ValidationError as CSRFError

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
                <a class="entry-link" href="/channels/{safe_cid}/messages" title="Browse and inspect messages for this channel"><strong>{safe_name}</strong></a> <span class="pill {load_class[h['load_state']]}" title="Channel health: {load_label[h['load_state']]}">{load_label[h['load_state']]}</span><br>
                <small style="color: #64748b;">in: {safe_ingestion_type} \u00b7 out: {safe_type}</small><br>
                <small>queue: <b style="color:{depth_color[h['load_state']]};" title="Messages waiting to be processed">{h['depth']}</b> \u00b7 <span title="Total retry attempts across messages">retries: {h['retries']}</span> \u00b7 <span title="Messages that failed all retries (dead-lettered)">dlq: {h['dead_letter']}</span> \u00b7 <span title="Messages currently being processed">in-flight: {h['in_flight']}</span></small><br>
                <small style="color: #475569;">{last_line}</small>
            </td>
            <td><span class="pill {pill_class}" title="Channel is {status_label.lower()}">{status_label}</span></td>
            <td>
                <div style="display: flex; gap: 4px; flex-wrap: wrap;">
                    <button class="{btn_class}"
                            hx-post="/ui/channels/{safe_cid}/{btn_action}"
                            hx-target="#channel-table-body"
                            title="{btn_label} message processing for this channel">
                        {btn_label}
                    </button>
                    <button class="btn"
                            hx-get="/ui/channels/{safe_cid}/edit"
                            hx-target="#editor-container"
                            hx-on:htmx:after-request="if(event.detail.successful) switchTab('editor')"
                            title="Modify this channel's configuration">
                        Edit
                    </button>
                    <button class="btn btn-danger"
                            hx-delete="/ui/channels/{safe_cid}"
                            hx-confirm="Are you sure you want to delete channel &#39;{safe_cid}&#39;?"
                            hx-target="#channel-table-body"
                            title="Permanently delete this channel and its message history">
                        Delete
                    </button>
                </div>
            </td>
        </tr>
        """
    if not rows_html:
        return ('<tr><td colspan="3" class="empty-state" style="padding:28px 20px;">'
                '<h3>No channels configured yet</h3>'
                '<p>A channel ingests data from a source (webhook, file, database), optionally '
                'transforms it with mappings and enrichments, and delivers it to a destination.</p>'
                '<button class="btn btn-primary" hx-get="/ui/channels/new" hx-target="#editor-container" '
                'hx-on:htmx:after-request="if(event.detail.successful) switchTab(\'editor\')">'
                '+ Create Your First Channel</button>'
                '</td></tr>')
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
        ("shared_secret", "Shared Secret", "password", "", "optional HMAC secret", False),
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

_SECRET_FIELD_TYPES = {"password"}


def _make_field_dict(name, label, ftype, default, placeholder, required, current_value=None):
    if ftype in _SECRET_FIELD_TYPES:
        # Never reflect the real stored secret back into the page.
        return {
            "name": name, "label": label, "ftype": ftype,
            "default": default, "placeholder": placeholder,
            "required": required,
            "value": "••••••" if current_value else "",
        }
    return {
        "name": name, "label": label, "ftype": ftype,
        "default": default, "placeholder": placeholder,
        "required": required,
        "value": current_value if current_value is not None else default,
    }


def _render_channel_form(config=None, flash_errors=None, flash_success=None, initial_step=0):
    """Build structured data and render the channel-form Jinja2 template.

    IMPORTANT: this must call render_template() exactly once. Never wrap its
    output in render_template_string() — that re-parses already-rendered
    HTML as Jinja2 source, which lets any user-controlled text already
    substituted into the page (channel name, field values, etc.) execute as
    a template expression on the second pass. That's an SSTI/RCE hole. Keep
    this function to a single render_template() call.
    """
    from nodes.codec import keys as codec_keys
    from api.ui.helpers import codec_label
    from core.field_catalog import catalog_for_codec
    from api.ui.fields import STEP_TYPE_META

    if config is None:
        config = {
            "channel_id": "", "name": "", "enabled": True, "status": "running",
            "concurrency": 1,
            "inbound_transport": "http_webhook", "inbound_transport_config": {},
            "inbound_codec": "json", "outbound_codec": "json",
            "destination": "http", "destination_config": {"endpoint_url": ""},
            "retry_policy_id": "default", "pipeline": [],
        }

    trans_cfg = config.get("inbound_transport_config") or {}
    dest_cfg = config.get("destination_config") or {}
    selected_transport = config.get("inbound_transport", "http_webhook")
    selected_dest = config.get("destination", "http")
    selected_codec = config.get("inbound_codec", "json")
    selected_out_codec = config.get("outbound_codec", "json")

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

    shared_steps = registry.list_shared_steps()
    retry_policies = [{"value": rp["retry_policy_id"], "label": rp["retry_policy_id"],
                       "selected": rp["retry_policy_id"] == config.get("retry_policy_id")}
                      for rp in registry.list_retry_policies()]

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

    pipeline = config.get("pipeline") or []
    if not isinstance(pipeline, list):
        pipeline = []

    # Serialize pipeline for the hidden field in the step-chain builder.
    pipeline_json = json.dumps(pipeline)

    # Build the field catalog for the selected inbound codec so the
    # step-chain builder can show friendly labels with codec hints.
    field_catalog_raw = catalog_for_codec(selected_codec)
    field_catalog = [
        {
            "path": d.path,
            "label": d.label,
            "kind": d.kind,
            "group": d.group,
            "hint": getattr(d, "_hint", None),
        }
        for d in field_catalog_raw
    ]

    tmpl_config = {
        "channel_id": config.get("channel_id", ""),
        "name": config.get("name", ""),
        "status": config.get("status", "running"),
        "concurrency": config.get("concurrency", 1),
        "enabled": config.get("enabled", True),
        "pipeline": json.dumps(pipeline, indent=2) if pipeline else "",
    }

    return render_template(
        "channel_form.html",
        config=tmpl_config,
        transports=transports,
        codecs=codecs,
        outbound_codecs=outbound_codecs,
        destinations=destinations,
        transport_groups=transport_groups,
        dest_groups=dest_groups,
        shared_steps=shared_steps,
        retry_policies=retry_policies,
        flash_errors=flash_errors,
        flash_success=flash_success,
        initial_step=initial_step,
        field_catalog=field_catalog,
        step_type_meta=STEP_TYPE_META,
        pipeline_json=pipeline_json,
    )


# ---------------------------------------------------------------------------
# Save channel
# ---------------------------------------------------------------------------

def _collect_prefixed(prefix, field_defs):
    """Read `{prefix}__{name}` inputs for one field-def list, coercing by the
    field's own declared type rather than guessing from the value's shape.
    """
    out = {}
    for name, _label, ftype, _default, _placeholder, _required in field_defs:
        raw = request.form.get(f"{prefix}__{name}")
        if ftype == "checkbox":
            if raw is not None:
                out[name] = True
        elif raw not in (None, ""):
            if ftype == "number":
                try:
                    out[name] = int(raw)
                except ValueError:
                    try:
                        out[name] = float(raw)
                    except ValueError:
                        out[name] = raw
            else:
                out[name] = raw
    return out


def _merge_secret_fields(new_cfg, old_cfg, field_defs):
    """A blank password field means 'leave unchanged', not 'clear it'."""
    if not old_cfg:
        return new_cfg
    for name, _label, ftype, _default, _placeholder, _required in field_defs:
        if ftype == "password" and name not in new_cfg and name in old_cfg:
            new_cfg[name] = old_cfg[name]
    return new_cfg


@bp.route("/ui/channels/save", methods=["POST"])
def save_channel():
    # CSRF check — remove this block if you haven't adopted Flask-WTF yet.
    try:
        validate_csrf(request.form.get("csrf_token", ""))
    except CSRFError:
        return _render_channel_form(
            flash_errors=["Your session expired or the form was tampered with. Please try again."],
        ), 400

    is_edit = request.form.get("is_edit") == "true"
    channel_id = request.form.get("channel_id", "").strip()
    wizard_step = request.form.get("wizard_step", "0")

    def _int_field(name, default):
        try:
            return int(request.form.get(name) or default)
        except ValueError:
            return default

    def _json_field(name, default=None):
        raw = request.form.get(name, "").strip()
        if not raw:
            return default
        try:
            return json.loads(raw)
        except Exception as e:
            raise ConfigValidationError(f"Invalid JSON for {name}: {e}")

    def _pipeline_from_request():
        pipeline = _json_field("pipeline", [])
        if pipeline is None:
            pipeline = []
        if not isinstance(pipeline, list):
            raise ConfigValidationError("pipeline must be a JSON array of steps")
        return pipeline

    inbound_transport = request.form.get("inbound_transport", "")
    destination = request.form.get("destination", "")

    existing = registry.load_all_configs().get(channel_id) if is_edit else None
    old_trans_cfg = (existing or {}).get("inbound_transport_config") or {}
    old_dest_cfg = (existing or {}).get("destination_config") or {}

    inbound_transport_config = _collect_prefixed(
        "tc", _TRANSPORT_FIELDS.get(inbound_transport, [])
    )
    destination_config = _collect_prefixed(
        "dc", _DESTINATION_FIELDS.get(destination, [])
    )
    inbound_transport_config = _merge_secret_fields(
        inbound_transport_config, old_trans_cfg, _TRANSPORT_FIELDS.get(inbound_transport, [])
    )
    destination_config = _merge_secret_fields(
        destination_config, old_dest_cfg, _DESTINATION_FIELDS.get(destination, [])
    )

    def _form_config_from_request():
        return {
            "channel_id": channel_id,
            "name": request.form.get("name", "").strip(),
            "enabled": request.form.get("enabled") == "on",
            "status": request.form.get("status", "running"),
            "concurrency": _int_field("concurrency", 1),
            "inbound_transport": inbound_transport,
            "inbound_transport_config": inbound_transport_config,
            "inbound_codec": request.form.get("inbound_codec", ""),
            "outbound_codec": request.form.get("outbound_codec", ""),
            "destination": destination,
            "destination_config": destination_config,
            "retry_policy_id": request.form.get("retry_policy_id", ""),
            "pipeline": _pipeline_from_request(),
        }

    if not is_edit and not CHANNEL_ID_RE.match(channel_id):
        return _render_channel_form(
            config=_form_config_from_request(),
            flash_errors=["Channel ID may only contain letters, numbers, underscores, and hyphens."],
            initial_step=wizard_step,
        )

    try:
        definition = {
            "channel_id": channel_id,
            "name": request.form.get("name", "").strip(),
            "enabled": request.form.get("enabled") == "on",
            "status": request.form.get("status", "running"),
            "concurrency": max(1, min(16, _int_field("concurrency", 1))),
            "inbound_transport": inbound_transport,
            "inbound_transport_config": inbound_transport_config,
            "inbound_codec": request.form.get("inbound_codec", ""),
            "outbound_codec": request.form.get("outbound_codec", ""),
            "destination": destination,
            "destination_config": destination_config,
            "retry_policy_id": request.form.get("retry_policy_id", "") or None,
            "pipeline": _pipeline_from_request(),
        }
        registry.save_channel_definition(definition)
    except ConfigValidationError as e:
        return _render_channel_form(
            config=_form_config_from_request(),
            flash_errors=str(e).split("; "),
            initial_step=wizard_step,
        )
    except Exception as e:
        return _render_channel_form(
            config=_form_config_from_request(),
            flash_errors=[f"Database Error: {str(e)}"],
            initial_step=wizard_step,
        )

    registry.load_all_configs()
    sync_webhooks()
    return (f'<div hx-swap-oob="innerHTML:#channel-table-body">{get_ui_channels()}</div>'
            + _render_channel_form(
                config=registry.load_all_configs().get(channel_id, _form_config_from_request()),
                flash_success=f'✓ Channel <code>{_esc(channel_id)}</code> saved successfully.',
            ))