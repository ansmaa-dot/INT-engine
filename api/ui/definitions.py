"""Definitions blueprint — shared pipeline steps and retry-policy management."""

import json

from flask import Blueprint, render_template, request

from api.deps import registry
from api.ui.helpers import _esc
from engine.config_loader import ConfigValidationError

bp = Blueprint("ui_defs", __name__)

_STEP_KINDS = ("mapping", "enrichment", "filter", "assert")

_KIND_LABELS = {
    "mapping": "Mapping (transform rules)",
    "enrichment": "Enrichment (DB lookup)",
    "filter": "Filter (boolean expression)",
    "assert": "Assert (validation expression)",
}


def _defs_body():
    """Returns the definitions page body fragment (shared by standalone page
    and HTMX panel load)."""
    steps = registry.list_shared_steps()
    retries = registry.list_retry_policies()

    step_rows = "".join(
        f'<tr><td>{_esc(s["kind"])}</td><td>{_esc(s["id"])}</td>'
        f'<td>v{s["version"]}</td><td>{_esc(s.get("description") or "")}</td></tr>'
        for s in steps) or '<tr><td colspan="4">none</td></tr>'
    retry_rows = "".join(
        f'<tr><td>{_esc(r["retry_policy_id"])}</td><td>{r["max_retries"]}</td><td>{r["base_backoff_seconds"]}</td></tr>'
        for r in retries) or '<tr><td colspan="3">none</td></tr>'

    kind_opts = "".join(
        f'<option value="{k}">{_esc(label)}</option>'
        for k, label in _KIND_LABELS.items())

    return f"""<div id="defs-panel-content" style="display:grid;grid-template-columns:1fr 1fr;gap:20px;">
    <div style="grid-column:1/-1;font-size:12px;color:var(--text-muted);margin-bottom:4px;line-height:1.5;">
        <strong>Shared steps</strong> are versioned, immutable building blocks that
        channels reference from their pipeline at a pinned version. Saving always
        creates a new version — existing channels keep the version they pinned.
        <strong>Retry policies</strong> control how many times a failed message is
        retried and how long to wait between attempts.
    </div>
    <div class="section-box" style="margin-bottom:0;">
        <div class="section-header-row"><h2 style="border-bottom:none;">Shared Steps</h2></div>
        <div class="section-body">
            <table class="data-table"><thead><tr><th>Kind</th><th>ID</th><th>Version</th><th>Desc</th></tr></thead><tbody>{step_rows}</tbody></table>
            <form hx-post="/ui/defs/steps/save" hx-target="#step-msg" hx-swap="innerHTML" style="margin-top:8px;">
                <select name="kind" required style="width:100%;margin-bottom:4px;">{kind_opts}</select>
                <input name="step_id" placeholder="step_id" required style="width:100%;margin-bottom:4px;">
                <input name="version" placeholder="version (blank=next)" type="number" min="1" style="width:100%;margin-bottom:4px;">
                <textarea name="config" placeholder='config JSON, e.g. {{"rules": [{{"source": "patient.name", "target": "patient.name"}}]}}' rows="4" style="width:100%;font-family:monospace;font-size:11px;margin-bottom:4px;"></textarea>
                <input name="description" placeholder="description" style="width:100%;margin-bottom:4px;">
                <button class="btn btn-primary" type="submit">Save Shared Step</button>
                <span id="step-msg"></span>
            </form>
        </div>
    </div>
    <div class="section-box" style="margin-bottom:0;">
        <div class="section-header-row"><h2 style="border-bottom:none;">Retry Policies</h2></div>
        <div class="section-body">
            <table class="data-table"><thead><tr><th>ID</th><th>Max Retries</th><th>Base Backoff (s)</th></tr></thead><tbody>{retry_rows}</tbody></table>
            <form hx-post="/ui/defs/retry/save" hx-target="#retry-msg" hx-swap="innerHTML" style="margin-top:8px;">
                <input name="retry_policy_id" placeholder="retry_policy_id" required style="width:100%;margin-bottom:4px;">
                <input name="max_retries" placeholder="max_retries" type="number" min="0" required style="width:100%;margin-bottom:4px;">
                <input name="base_backoff_seconds" placeholder="base_backoff_seconds" type="number" min="0" required style="width:100%;margin-bottom:4px;">
                <input name="description" placeholder="description" style="width:100%;margin-bottom:4px;">
                <button class="btn btn-primary" type="submit">Save Retry Policy</button>
                <span id="retry-msg"></span>
            </form>
        </div>
    </div></div>"""


@bp.route("/ui/defs")
def defs_page():
    body = _defs_body()
    if request.headers.get("HX-Request"):
        return body
    return render_template("definitions.html", body=body)


@bp.route("/ui/defs/steps/save", methods=["POST"])
def save_shared_step_def():
    try:
        kind = request.form.get("kind", "").strip()
        if kind not in _STEP_KINDS:
            raise ConfigValidationError(
                f"kind must be one of: {', '.join(_STEP_KINDS)}")
        config = json.loads(request.form.get("config", "") or "")
        if not isinstance(config, dict):
            raise ConfigValidationError("config must be a JSON object")
        version = registry.save_shared_step(
            kind,
            request.form.get("step_id", "").strip(),
            config,
            description=request.form.get("description", "").strip(),
            version=int(request.form.get("version") or 0) or None,
        )
    except ConfigValidationError as e:
        return f'<span style="color: var(--status-red);">{_esc(str(e))}</span>', 400
    except json.JSONDecodeError as e:
        return f'<span style="color: var(--status-red);">Invalid JSON for config: {_esc(str(e))}</span>', 400
    return (f'<span style="color: var(--status-green);">&#10003; Saved shared step v{version}.</span>'
            f'<div hx-swap-oob="innerHTML:#defs-panel-content">{_defs_body()}</div>')


@bp.route("/ui/defs/retry/save", methods=["POST"])
def save_retry_def():
    try:
        registry.save_retry_policy(
            retry_policy_id=request.form.get("retry_policy_id", "").strip(),
            max_retries=int(request.form.get("max_retries", 0)),
            base_backoff_seconds=int(request.form.get("base_backoff_seconds", 0)),
            description=request.form.get("description", "").strip(),
        )
    except ConfigValidationError as e:
        return f'<span style="color: var(--status-red);">{_esc(str(e))}</span>', 400
    return (f'<span style="color: var(--status-green);">&#10003; Saved retry policy.</span>'
            f'<div hx-swap-oob="innerHTML:#defs-panel-content">{_defs_body()}</div>')
