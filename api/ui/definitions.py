"""Definitions blueprint — mapping, enrichment, and retry-policy management."""

import json

from flask import Blueprint, render_template, render_template_string, request

from api.deps import registry
from api.ui.helpers import _esc
from engine.config_loader import ConfigValidationError

bp = Blueprint("ui_defs", __name__)


def _defs_body():
    """Returns the definitions page body fragment (shared by standalone page
    and HTMX panel load)."""
    mappings = registry.list_mappings()
    enrichments = registry.list_enrichments()
    retries = registry.list_retry_policies()

    mapping_rows = "".join(
        f'<tr><td>{_esc(m["mapping_id"])}</td><td>v{m["version"]}</td><td>{_esc(m.get("description") or "")}</td></tr>'
        for m in mappings) or '<tr><td colspan="3">none</td></tr>'
    erich_rows = "".join(
        f'<tr><td>{_esc(e["enrichment_id"])}</td><td>v{e["version"]}</td>'
        f'<td>{_esc(e.get("db_type") or "sqlite")}</td>'
        f'<td>{_esc(e.get("description") or "")}</td></tr>'
        for e in enrichments) or '<tr><td colspan="4">none</td></tr>'
    retry_rows = "".join(
        f'<tr><td>{_esc(r["retry_policy_id"])}</td><td>{r["max_retries"]}</td><td>{r["base_backoff_seconds"]}</td></tr>'
        for r in retries) or '<tr><td colspan="3">none</td></tr>'

    return f"""<div id="defs-panel-content" style="display:grid;grid-template-columns:1fr 1fr;gap:20px;">
    <div style="grid-column:1/-1;font-size:12px;color:var(--text-muted);margin-bottom:4px;line-height:1.5;">
        <strong>Definitions</strong> are reusable building blocks that channels reference. 
        <span title="Reshape field names and values between source and destination formats">Mappings</span> transform data, 
        <span title="Look up additional data from external databases during processing">Enrichments</span> add context, and 
        <span title="Control how many times a failed message is retried and how long to wait between attempts">Retry Policies</span> handle failures.
    </div>
    <div class="section-box" style="margin-bottom:0;">
        <div class="section-header-row"><h2 style="border-bottom:none;">Mappings</h2></div>
        <div class="section-body">
            <table class="data-table"><thead><tr><th>ID</th><th>Version</th><th>Desc</th></tr></thead><tbody>{mapping_rows}</tbody></table>
            <form hx-post="/ui/defs/mappings/save" hx-target="#mapping-msg" hx-swap="innerHTML" style="margin-top:8px;">
                <input name="mapping_id" placeholder="mapping_id" required style="width:100%;margin-bottom:4px;">
                <input name="version" placeholder="version (blank=next)" type="number" min="1" style="width:100%;margin-bottom:4px;">
                <textarea name="rules" placeholder='[{{"source":"patient.name","target":"patient.name","fn":"Uppercase"}}]' rows="4" style="width:100%;font-family:monospace;font-size:11px;margin-bottom:4px;"></textarea>
                <input name="description" placeholder="description" style="width:100%;margin-bottom:4px;">
                <button class="btn btn-primary" type="submit">Save Mapping</button>
                <span id="mapping-msg"></span>
            </form>
        </div>
    </div>
    <div class="section-box" style="margin-bottom:0;">
        <div class="section-header-row"><h2 style="border-bottom:none;">Enrichments</h2></div>
        <div class="section-body">
            <table class="data-table"><thead><tr><th>ID</th><th>Version</th><th>DB</th><th>Desc</th></tr></thead><tbody>{erich_rows}</tbody></table>
            <form hx-post="/ui/defs/enrichments/save" hx-target="#erich-msg" hx-swap="innerHTML" style="margin-top:8px;">
                <input name="enrichment_id" placeholder="enrichment_id" required style="width:100%;margin-bottom:4px;">
                <input name="source_key_field" placeholder="source_key_field (canonical path)" required style="width:100%;margin-bottom:4px;">
                <input name="lookup_db_path" placeholder="lookup_db_path (SQLite path or file)" required style="width:100%;margin-bottom:4px;">
                <select name="db_type" style="width:100%;margin-bottom:4px;padding:6px 8px;border:1px solid #cbd5e1;border-radius:4px;font-size:12px;">
                    <option value="sqlite">SQLite</option>
                    <option value="postgresql">PostgreSQL</option>
                    <option value="mysql">MySQL</option>
                </select>
                <input name="connection_string" placeholder="connection_string (PostgreSQL: postgresql://... / MySQL: leave blank, use above path)" style="width:100%;margin-bottom:4px;">
                <input name="target_table" placeholder="target_table" required style="width:100%;margin-bottom:4px;">
                <input name="target_key_col" placeholder="target_key_col" required style="width:100%;margin-bottom:4px;">
                <input name="fields" placeholder='["field1","field2"]' required style="width:100%;margin-bottom:4px;">
                <input name="lookup_name" placeholder="lookup_name" required style="width:100%;margin-bottom:4px;">
                <input name="description" placeholder="description (optional)" style="width:100%;margin-bottom:4px;">
                <input name="version" placeholder="version (blank=next)" type="number" min="1" style="width:100%;margin-bottom:4px;">
                <button class="btn btn-primary" type="submit">Save Enrichment</button>
                <span id="erich-msg"></span>
            </form>
        </div>
    </div>
    </div>
    <div class="section-box" style="margin-top:16px;">
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
    </div>"""


@bp.route("/ui/defs")
def defs_page():
    body = _defs_body()
    if request.headers.get("HX-Request"):
        return body
    return render_template("definitions.html", body=body)

@bp.route("/ui/defs/mappings/save", methods=["POST"])
def save_mapping_def():
    try:
        rules = json.loads(request.form.get("rules", "[]") or "[]")
        version = registry.save_mapping(
            mapping_id=request.form.get("mapping_id", "").strip(),
            rules=rules,
            description=request.form.get("description", "").strip(),
            version=int(request.form.get("version") or 0) or None,
        )
    except ConfigValidationError as e:
        return f'<span style="color: var(--status-red);">{_esc(str(e))}</span>', 400
    return (f'<span style="color: var(--status-green);">&#10003; Saved mapping v{version}.</span>'
            f'<div hx-swap-oob="innerHTML:#defs-panel-content">{_defs_body()}</div>')


@bp.route("/ui/defs/enrichments/save", methods=["POST"])
def save_enrichment_def():
    try:
        fields = json.loads(request.form.get("fields", "[]") or "[]")
        version = registry.save_enrichment(
            enrichment_id=request.form.get("enrichment_id", "").strip(),
            source_key_field=request.form.get("source_key_field", "").strip(),
            lookup_db_path=request.form.get("lookup_db_path", "").strip(),
            target_table=request.form.get("target_table", "").strip(),
            target_key_col=request.form.get("target_key_col", "").strip(),
            fields=fields,
            lookup_name=request.form.get("lookup_name", "").strip(),
            description=request.form.get("description", "").strip(),
            version=int(request.form.get("version") or 0) or None,
            db_type=request.form.get("db_type", "sqlite").strip(),
            connection_string=request.form.get("connection_string", "").strip() or None,
        )
    except ConfigValidationError as e:
        return f'<span style="color: var(--status-red);">{_esc(str(e))}</span>', 400
    return (f'<span style="color: var(--status-green);">&#10003; Saved enrichment v{version}.</span>'
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
