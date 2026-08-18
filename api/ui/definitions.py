"""Definitions blueprint — mapping, enrichment, and retry-policy management."""

import json

from flask import Blueprint, render_template_string, request

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
        f'<tr><td>{_esc(e["enrichment_id"])}</td><td>v{e["version"]}</td><td>{_esc(e.get("description") or "")}</td></tr>'
        for e in enrichments) or '<tr><td colspan="3">none</td></tr>'
    retry_rows = "".join(
        f'<tr><td>{_esc(r["retry_policy_id"])}</td><td>{r["max_retries"]}</td><td>{r["base_backoff_seconds"]}</td></tr>'
        for r in retries) or '<tr><td colspan="3">none</td></tr>'

    return f"""<div id="defs-panel-content" style="display:grid;grid-template-columns:1fr 1fr;gap:20px;">
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
            <table class="data-table"><thead><tr><th>ID</th><th>Version</th><th>Desc</th></tr></thead><tbody>{erich_rows}</tbody></table>
            <form hx-post="/ui/defs/enrichments/save" hx-target="#erich-msg" hx-swap="innerHTML" style="margin-top:8px;">
                <input name="enrichment_id" placeholder="enrichment_id" required style="width:100%;margin-bottom:4px;">
                <input name="source_key_field" placeholder="source_key_field (canonical path)" required style="width:100%;margin-bottom:4px;">
                <input name="lookup_db_path" placeholder="lookup_db_path" required style="width:100%;margin-bottom:4px;">
                <input name="target_table" placeholder="target_table" required style="width:100%;margin-bottom:4px;">
                <input name="target_key_col" placeholder="target_key_col" required style="width:100%;margin-bottom:4px;">
                <input name="fields" placeholder='["field1","field2"]' required style="width:100%;margin-bottom:4px;">
                <input name="lookup_name" placeholder="lookup_name" required style="width:100%;margin-bottom:4px;">
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
    return render_template_string("""
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <title>Definitions \u2014 Integration Engine</title>
            <style>
                :root {
                    --bg-page: #e2e8f0; --panel-bg: #f8fafc; --accent-blue: #0284c7;
                    --accent-blue-hover: #0369a1; --status-green: #16a34a;
                    --text-main: #0f172a; --text-muted: #475569;
                }
                * { box-sizing: border-box; margin: 0; padding: 0; }
                body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; background-color: var(--bg-page); color: var(--text-main); font-size: 14px; padding: 20px; }
                .page-wrap { max-width: 1400px; margin: 0 auto; }
                header { background: linear-gradient(180deg, #334155 0%, #0f172a 100%); color: #fff; padding: 14px 22px; border-radius: 5px; box-shadow: 0 2px 6px rgba(0,0,0,0.3); display: flex; justify-content: space-between; align-items: center; margin-bottom: 18px; border-bottom: 2px solid var(--accent-blue); }
                header h1 { font-size: 18px; font-weight: bold; text-shadow: 1px 1px 2px #000; }
                .btn { background: linear-gradient(180deg, #f1f5f9 0%, #e2e8f0 100%); border: 1px solid #94a3b8; padding: 6px 12px; border-radius: 4px; cursor: pointer; font-weight: bold; text-decoration: none; color: var(--text-main); font-size: 12px; }
                .btn-primary { background: linear-gradient(180deg, var(--accent-blue) 0%, var(--accent-blue-hover) 100%); border: 1px solid var(--accent-blue-hover); color: #fff; }
                .section-box { border: 1px solid #cbd5e1; border-radius: 4px; background: #f8fafc; overflow: hidden; margin-bottom: 18px; }
                .section-box h2, .section-header-row { background: linear-gradient(180deg, #f1f5f9 0%, #e2e8f0 100%); border-bottom: 1px solid #cbd5e1; padding: 9px 14px; font-size: 13px; font-weight: bold; color: #334155; }
                .section-header-row { display: flex; justify-content: space-between; align-items: center; padding-right: 14px; }
                .section-body { padding: 16px; }
                table.data-table { width: 100%; border-collapse: collapse; font-size: 12px; background: #fff; }
                table.data-table th { background: linear-gradient(180deg, #f8fafc 0%, #e2e8f0 100%); color: #334155; text-align: left; padding: 9px 12px; border: 1px solid #cbd5e1; font-weight: bold; }
                table.data-table td { padding: 9px 12px; border: 1px solid #e2e8f0; }
                input, textarea, select { padding: 6px 8px; border: 1px solid #cbd5e1; border-radius: 4px; font-size: 12px; }
                @media (max-width: 900px) { body > div[style*="grid-template-columns:1fr 1fr"] { grid-template-columns: 1fr !important; } }
            </style>
        </head>
        <body>
          <div class="page-wrap">
            <header>
                <h1><span>&#9881;</span> Reusable Definitions</h1>
                <a class="btn btn-primary" href="/">&#8592; Dashboard</a>
            </header>
            """ + body + """
          </div>
        </body>
        </html>
    """)


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
