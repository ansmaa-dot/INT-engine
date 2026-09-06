"""Channel messages blueprint — per-channel message browsing and inspection."""

from flask import Blueprint, render_template, request

from api.deps import registry, queue
from api.ui.helpers import _esc, ago, codec_label, pretty_payload, payload_preview, error_dict, error_preview
from api.ui.helpers import _MESSAGE_STATE_PILL

bp = Blueprint("ui_messages", __name__)


@bp.route("/channels/<channel_id>/messages")
def channel_messages_page(channel_id):
    configs = registry.load_all_configs()
    info = configs.get(channel_id)
    if not info:
        return app.response_class(
            status=404,
            response='<h1 style="padding:20px;">Channel not found</h1>'
                     '<p style="padding:0 20px;"><a href="/">Back to Dashboard</a></p>',
        )
    from api.ui.helpers import codec_label
    from api.ui.channels import _TRANSPORT_LABELS, _DEST_LABELS

    inbound_transport = info.get("inbound_transport", "unknown")
    inbound_codec = info.get("inbound_codec", "unknown")
    outbound_codec = info.get("outbound_codec", "unknown")
    destination = info.get("destination", "unknown")
    concurrency = info.get("concurrency", 1)
    retry_policy_id = info.get("retry_policy_id", "default")

    transport_label = _TRANSPORT_LABELS.get(inbound_transport, inbound_transport)
    dest_label = _DEST_LABELS.get(destination, destination)
    in_codec_label = codec_label(inbound_codec)
    out_codec_label = codec_label(outbound_codec)

    # Build a one-line summary of the transport config
    tc = info.get("inbound_transport_config") or {}
    dc = info.get("destination_config") or {}
    transport_detail = ""
    if inbound_transport == "mllp":
        transport_detail = f"{tc.get('host', '0.0.0.0')}:{tc.get('port', '?')}"
    elif inbound_transport == "http_webhook":
        transport_detail = f"POST /webhooks/{channel_id}"
    elif inbound_transport == "http_poller":
        transport_detail = tc.get("url", "?")
    elif inbound_transport == "file_watcher":
        transport_detail = tc.get("directory", "?")
    elif inbound_transport == "db_poller":
        transport_detail = tc.get("connection_string", "?")
    dest_detail = ""
    if destination == "http":
        dest_detail = dc.get("endpoint_url", "?")
    elif destination == "mllp":
        dest_detail = f"{dc.get('host', '?')}:{dc.get('port', '?')}"
    elif destination == "sftp":
        dest_detail = f"{dc.get('host', '?')}:{dc.get('port', 22)}"

    return render_template(
        "channel_messages.html",
        channel_id=channel_id,
        channel_name=info.get("name", channel_id),
        channel_status=info.get("status", "running"),
        inbound_transport=inbound_transport,
        transport_label=transport_label,
        transport_detail=transport_detail,
        inbound_codec=inbound_codec,
        in_codec_label=in_codec_label,
        outbound_codec=outbound_codec,
        out_codec_label=out_codec_label,
        destination=destination,
        dest_label=dest_label,
        dest_detail=dest_detail,
        concurrency=concurrency,
        retry_policy_id=retry_policy_id,
    )


@bp.route("/channels/<channel_id>/messages/body")
def get_channel_messages(channel_id):
    with queue._get_conn() as conn:
        rows = conn.execute(
            "SELECT trace_id, state, attempts, created_at, error, raw "
            "FROM queue WHERE channel_id = ? ORDER BY created_at DESC LIMIT 50",
            (channel_id,),
        ).fetchall()

    if not rows:
        return ('<tr><td colspan="6" style="text-align:center; color: var(--text-muted); padding: 12px;">'
                'No messages yet for this channel.</td></tr>')

    rows_html = ""
    for r in rows:
        safe_cid = _esc(channel_id, quote=True)
        safe_trace_id = _esc(r["trace_id"], quote=True)
        safe_trace_short = _esc(r["trace_id"][:12])
        state = (r["state"] or "UNKNOWN").upper()
        pill_class, pill_label = _MESSAGE_STATE_PILL.get(state, ("pill-paused", state))
        safe_pill = _esc(pill_label)
        safe_attempts = _esc(str(r["attempts"] or 0))
        safe_received = _esc(ago(r["created_at"]))
        safe_preview = _esc(payload_preview(r["raw"]))
        safe_err = _esc(error_preview(r["error"]))
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


@bp.route("/channels/<channel_id>/messages/<trace_id>")
def get_channel_message_detail(channel_id, trace_id):
    with queue._get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM queue WHERE trace_id = ? AND channel_id = ?",
            (trace_id, channel_id),
        ).fetchone()

    if not row:
        return '<div class="code-block">Message not found for this channel.</div>'

    state = (row["state"] or "UNKNOWN").upper()
    pill_class, pill_label = _MESSAGE_STATE_PILL.get(state, ("pill-paused", state))

    safe_trace = _esc(row["trace_id"], quote=True)
    safe_pill = _esc(pill_label)
    safe_attempts = _esc(str(row["attempts"] or 0))
    safe_created = _esc(ago(row["created_at"]))
    safe_idem = _esc(row["idempotency_key"] or "\u2014")

    configs = registry.load_all_configs()
    conf = configs.get(channel_id) or {}
    safe_in_label = _esc(codec_label(row["inbound_codec"]))
    safe_out_label = _esc(codec_label((conf.get("outbound_codec")) or ""))

    out = [f"""<div class="msg-meta">
            <code style="font-size:11px;">{safe_trace}</code>
            <span class="pill {pill_class}">{safe_pill}</span>
            <span>attempts: <b>{safe_attempts}</b></span>
            <span>received: {safe_created}</span>
            <span>idempotency key: <code style="font-size:10px;">{safe_idem}</code></span>
        </div>"""]

    if row["error"]:
        err = error_dict(row["error"]) or {}
        safe_err = _esc(err.get("message", ""))
        safe_stage = _esc(err.get("stage", ""))
        banner = f'<div class="error-banner"><b>Last error [{safe_stage}]:</b> {safe_err}</div>'
        if err.get("traceback"):
            safe_traceback = _esc(err["traceback"])
            banner += (f'<details class="trace-details"><summary>Stack trace</summary>'
                       f'<pre class="trace-block">{safe_traceback}</pre></details>')
        out.append(banner)
    else:
        out.append('<div class="error-banner banner-ok"><b>No processing errors.</b></div>')

    source_pane = f'<div class="code-block-dark">{_esc(pretty_payload(row["raw"]))}</div>'

    transformed_pretty = pretty_payload(row["canonical"])
    if transformed_pretty is not None:
        dest_pane = f'<div class="code-block-dark dest-sent">{_esc(transformed_pretty)}</div>'
    else:
        dest_pane = ('<div class="code-block-dark dim-block">No outbound payload \u2014 '
                     'processing failed before reaching the destination.</div>')

    out.append(f"""<div class="grid-2col pane-grid">
        <div class="section-box pane">
            <div class="section-header-row pane-header">
                <span class="pane-title">Source</span>
                <span class="pane-sub">raw inbound payload · <code class="pane-codec">{safe_in_label}</code></span>
            </div>
            <div style="padding: 12px;">{source_pane}</div>
        </div>
        <div class="section-box pane">
            <div class="section-header-row pane-header">
                <span class="pane-title">Destination</span>
                <span class="pane-sub">transformed outbound payload · <code class="pane-codec">{safe_out_label}</code></span>
            </div>
            <div style="padding: 12px;">{dest_pane}</div>
        </div>
    </div>""")

    return "".join(out)
