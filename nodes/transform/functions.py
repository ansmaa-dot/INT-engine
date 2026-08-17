"""
Whitelisted transform functions only — no eval, no arbitrary scripting.
This is what actually enforces the "no dynamic scripting engine" rule: a bad
mapping config can reference an unknown function name and fail loudly, but it
can never inject code.
"""
from datetime import datetime


def fn_uppercase(v, **kwargs):
    return str(v).upper() if v is not None else v


def fn_lowercase(v, **kwargs):
    return str(v).lower() if v is not None else v


def fn_trim(v, **kwargs):
    return str(v).strip() if v is not None else v


def fn_date_format(v, fmt="%Y%m%d%H%M%S", **kwargs):
    if isinstance(v, datetime):
        return v.strftime(fmt)
    if isinstance(v, str):
        # best-effort passthrough parse for ISO-ish input strings
        try:
            return datetime.fromisoformat(v).strftime(fmt)
        except ValueError:
            return v
    return v


def fn_default(v, default=None, **kwargs):
    return v if v is not None else default


REGISTRY = {
    "Uppercase": fn_uppercase,
    "Lowercase": fn_lowercase,
    "Trim Whitespace": fn_trim,
    "Format": fn_date_format,
    "Default": fn_default,
}
