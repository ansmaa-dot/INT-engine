"""DLQ & Audit blueprint — dead-letter queue management and audit trail lookup."""

import json

from flask import Blueprint, jsonify, request

from api.deps import registry, queue
from api.ui.helpers import _esc, ago, pretty_payload, error_preview, error_dict

bp = Blueprint("ui_dlq", __name__)


@bp.route("/ui/dlq")
def get_ui_dlq():
    with queue._get_conn() as conn:
        rows = conn.execute(
            "SELECT trace_id, channel_id, created_at, error FROM queue WHERE UPPER(state) = 'DEAD_LETTER'"
        ).fetchall()

    if not rows:
        return '<tr><td colspan="4" style="text-align:center; color: var(--text-muted); padding: 12px;">No DLQ records found.</td></tr>'

    rows_html = ""
    for r in rows:
        safe_trace_id = _esc(r["trace_id"], quote=True)
        safe_trace_id_short = _esc(r["trace_id"][:12])
        safe_last_error = _esc(error_preview(r["error"]))
        last_seen = ago(r["created_at"])
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


@bp.route("/ui/dlq/<trace_id>")
def get_dlq_detail(trace_id):
    with queue._get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM queue WHERE trace_id = ?", (trace_id,)
        ).fetchone()

    if not row:
        return '<div class="code-block">Record not found</div>'

    raw = row["raw"] or ""
    try:
        if raw.startswith(("{", "[")):
            raw = json.loads(raw)
            raw = json.dumps(raw, indent=2)
    except Exception:
        pass

    error = row["error"] or ""
    try:
        if error:
            error = json.loads(error)
            error = json.dumps(error, indent=2)
    except Exception:
        pass

    return f"""<div class="code-block" style="max-height:400px; overflow-y:auto; margin-bottom:12px;">
<span style="color:#38bdf8;"><b>Payload</b></span>
{_esc(raw)}
</div>
<div class="code-block" style="max-height:400px; overflow-y:auto;">
<span style="color:#f87171;"><b>Error</b></span>
{_esc(error if error else '(none)')}
</div>"""


@bp.route("/ui/audit")
def get_audit_ui():
    trace_id = request.args.get("trace_id", "").strip()
    if not trace_id:
        return '<div class="code-block">Enter a trace_id above and click "Trace".</div>'

    with queue._get_conn() as conn:
        row = conn.execute("SELECT * FROM queue WHERE trace_id = ?", (trace_id,)).fetchone()
        trail = queue.get_audit_trail(trace_id)

    if not row and not trail:
        return f'<div class="error-banner">No records found for trace_id <code>{_esc(trace_id)}</code>.</div>'

    safe_trace = _esc(trace_id)
    parts = [f'<div class="msg-meta"><code style="font-size:11px;">{safe_trace}</code></div>']

    if row:
        raw_pretty = pretty_payload(row["raw"])
        canonical_pretty = pretty_payload(row["canonical"])
        parts.append('<div class="grid-2col pane-grid">'
                     f'<div class="section-box pane"><div class="section-header-row"><span class="pane-title">Raw Payload</span></div><div class="code-block">{_esc(raw_pretty or "(empty)")}</div></div>'
                     f'<div class="section-box pane"><div class="section-header-row"><span class="pane-title">Canonical</span></div><div class="code-block">{_esc(canonical_pretty or "(not reached)")}</div></div>'
                     '</div>')

    if trail:
        trail_html = "".join(
            f'<tr><td>{_esc(e["event"])}</td><td>{_esc(e["at"])}</td></tr>'
            for e in trail
        )
        parts.append(
            '<div class="section-box" style="margin-top:12px;">'
            '<div class="section-header-row"><span class="pane-title">Audit Trail</span></div>'
            '<table class="data-table"><thead><tr><th>Event</th><th>Timestamp</th></tr></thead>'
            f'<tbody>{trail_html}</tbody></table></div>'
        )

    return "".join(parts)


@bp.route("/api/audit/<trace_id>")
def get_audit_api(trace_id):
    trail = queue.get_audit_trail(trace_id)
    return jsonify({"trace_id": trace_id, "events": trail})


@bp.route("/ui/dlq/clear", methods=["POST"])
def clear_dlq():
    with queue._get_conn() as conn:
        conn.execute("DELETE FROM queue WHERE UPPER(state) = 'DEAD_LETTER'")
        conn.commit()
    return get_ui_dlq()


@bp.route("/ui/dlq/requeue", methods=["POST"])
def requeue_dlq():
    with queue._get_conn() as conn:
        conn.execute(
            "UPDATE queue SET state = 'QUEUED', attempts = 0, error = NULL, next_retry_at = NULL WHERE UPPER(state) = 'DEAD_LETTER'"
        )
        conn.commit()
    return get_ui_dlq()


@bp.route("/ui/dlq/<trace_id>/requeue", methods=["POST"])
def requeue_dlq_entry(trace_id):
    with queue._get_conn() as conn:
        conn.execute(
            "UPDATE queue SET state = 'QUEUED', attempts = 0, error = NULL, next_retry_at = NULL WHERE trace_id = ? AND UPPER(state) = 'DEAD_LETTER'",
            (trace_id,),
        )
        conn.commit()
    return get_ui_dlq()


@bp.route("/ui/dlq/<trace_id>", methods=["DELETE"])
def discard_dlq_entry(trace_id):
    with queue._get_conn() as conn:
        conn.execute("DELETE FROM queue WHERE trace_id = ? AND UPPER(state) = 'DEAD_LETTER'", (trace_id,))
        conn.commit()
    return get_ui_dlq()
