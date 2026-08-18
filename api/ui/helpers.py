"""UI utility helpers — HTML escaping, time formatting, payload pretty-printing,
error inspection, and channel-health aggregation."""

import html as _html_mod
import json
from datetime import datetime, timedelta, timezone

_html_escape = _html_mod.escape
_esc = _html_escape


def iso_ago(seconds: float) -> str:
    return (datetime.now(timezone.utc) - timedelta(seconds=seconds)).isoformat()


def ago(iso_str: str | None) -> str:
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


def codec_label(key: str) -> str:
    if not key:
        return "unknown"
    m = {"json": "JSON", "hl7v2": "HL7 v2.x", "fhir_r4_json": "FHIR R4 (JSON)",
         "csv": "CSV", "xml": "XML",
         "hl7v2.5.1.oru_r01": "HL7 v2.5.1 ORU_R01",
         "hl7v2.5.1.adt_a01": "HL7 v2.5.1 ADT_A01",
         "hl7v2.5.1.orm_o01": "HL7 v2.5.1 ORM_O01",
         "hl7v2.5.1.undefined": "HL7 v2.5.1 (any)"}
    return m.get(key.lower(), key)


def is_hl7_like(text: str) -> bool:
    return bool(text) and text.startswith("MSH|")


def pretty_payload(raw_str) -> str | None:
    if raw_str is None:
        return None
    try:
        obj = json.loads(raw_str)
        return json.dumps(obj, indent=2)
    except (TypeError, ValueError):
        return str(raw_str)


def payload_preview(raw_str, limit: int = 60) -> str:
    pretty = pretty_payload(raw_str)
    if pretty is None:
        return "—"
    if len(pretty) > limit:
        return pretty[:limit] + "…"
    return pretty


def error_dict(error_json) -> dict | None:
    if not error_json:
        return None
    try:
        return json.loads(error_json)
    except (TypeError, ValueError):
        return None


def error_preview(error_json, limit: int = 80) -> str:
    err = error_dict(error_json) or {}
    msg = err.get("message", "")
    if not msg:
        return "—"
    if len(msg) > limit:
        return msg[:limit] + "…"
    return msg


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


def channel_health(configs: dict, queue) -> dict:
    health = {cid: dict(_DEFAULT_HEALTH) for cid in configs}
    with queue._get_conn() as conn:
        for r in conn.execute("""
            SELECT channel_id,
                SUM(CASE WHEN UPPER(state)='QUEUED' THEN 1 ELSE 0 END) AS depth,
                SUM(CASE WHEN UPPER(state)='PROCESSING' THEN 1 ELSE 0 END) AS in_flight,
                SUM(CASE WHEN UPPER(state)='DELIVERED' THEN 1 ELSE 0 END) AS delivered,
                SUM(CASE WHEN UPPER(state)='DEAD_LETTER' THEN 1 ELSE 0 END) AS dead_letter
            FROM queue GROUP BY channel_id"""):
            h = health.setdefault(r["channel_id"], dict(_DEFAULT_HEALTH))
            h["depth"] = r["depth"] or 0
            h["in_flight"] = r["in_flight"] or 0
            h["delivered_cache"] = r["delivered"] or 0
            h["dead_letter"] = r["dead_letter"] or 0
        for r in conn.execute("""
            SELECT channel_id,
                COALESCE(SUM(CASE WHEN event='retry_scheduled' THEN 1 ELSE 0 END), 0) AS retries,
                MAX(CASE WHEN event='delivered' THEN at END) AS last_delivered_at,
                MAX(CASE WHEN event IN ('retry_scheduled','dead_lettered') THEN at END) AS last_error_at
            FROM audit_log GROUP BY channel_id"""):
            h = health.setdefault(r["channel_id"], dict(_DEFAULT_HEALTH))
            h["retries"] = r["retries"] or 0
            h["last_delivered_at"] = r["last_delivered_at"]
            h["last_error_at"] = r["last_error_at"]
        cutoff = iso_ago(300)
        for r in conn.execute("""
            SELECT channel_id, COUNT(*) AS n
            FROM audit_log WHERE event='delivered' AND at >= ?
            GROUP BY channel_id""", (cutoff,)):
            h = health.setdefault(r["channel_id"], dict(_DEFAULT_HEALTH))
            h["delivered_5m"] = r["n"] or 0
            h["dnpm"] = (r["n"] or 0) / 5.0
    for cid, h in health.items():
        max_depth = (configs.get(cid, {}).get("inbound_transport_config") or {}).get("max_queue_depth")
        load = _load_state(h["depth"], max_depth)
        if h["dead_letter"] > 0:
            load = "error"
        elif h["retries"] > 0 and load == "ok":
            load = "warn"
        h["load_state"] = load
    return health
