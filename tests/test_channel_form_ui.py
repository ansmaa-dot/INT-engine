"""Channel editor UI smoke tests.

The Map step builder must expose the whitelisted transform functions so
``fn`` / ``fn_args`` round-trip through the form. This renders
``_render_channel_form`` through Jinja and asserts the function registry
appears in the emitted markup.
"""

import os

import pytest
from flask import Flask

import api.ui.channels as ch
from engine.config_loader import ChannelConfigRegistry


_TEMPLATES = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "..", "api", "ui", "templates",
)


def _render_channel_form(tmp_path, pipeline=None):
    app = Flask(__name__, template_folder=_TEMPLATES)
    app.config["SECRET_KEY"] = "test"
    # The form template renders a CSRF token via Flask-WTF; provide a stub so
    # the test only exercises the channel-form markup.
    app.jinja_env.globals["csrf_token"] = lambda: "test-token"

    reg = ChannelConfigRegistry(str(tmp_path / "ui.db"))
    old_registry, old_queue = ch.registry, ch.queue
    ch.registry, ch.queue = reg, None
    try:
        with app.app_context():
            return ch._render_channel_form(config={
                "channel_id": "c1", "name": "C1", "enabled": True, "status": "running",
                "concurrency": 1,
                "inbound_transport": "http_webhook", "inbound_transport_config": {},
                "inbound_codec": "schemaless.json", "outbound_codec": "hl7v2.5.1.ORM_O01",
                "destination": "http", "destination_config": {"endpoint_url": "http://x"},
                "retry_policy_id": "default", "pipeline": pipeline or [],
            })
    finally:
        ch.registry, ch.queue = old_registry, old_queue


def test_channel_form_lists_transform_functions(tmp_path):
    html = _render_channel_form(tmp_path)
    for fn in ("Uppercase", "Lowercase", "Trim Whitespace", "Format", "Default"):
        assert fn in html
    assert "Args" in html
    assert 'class="cf-input rule-fn"' in html


def test_channel_form_round_trips_transform_rule_functions(tmp_path):
    """A stored transform rule with fn + fn_args must survive the form render:
    the hidden pipeline field (which the JS re-hydration reads) keeps fn and
    fn_args, and the renderer binds the stored fn back into the function
    <select>."""
    html = _render_channel_form(tmp_path, pipeline=[{
        "step_id": "t1", "type": "transform",
        "config": {"rules": [
            {"source": "extensions.patient_firstname", "target": "patient.name",
             "fn": "Uppercase"},
            {"source": "patient.name", "target": "patient.name",
             "fn": "Format", "fn_args": {"fmt": "%Y-%m-%d"}},
        ]},
    }])
    # (1) the TRANSFORM_FNS script var carries every whitelisted function
    assert "var TRANSFORM_FNS = " in html
    for fn in ("Uppercase", "Lowercase", "Trim Whitespace", "Format", "Default"):
        assert fn in html
    # (2) the hidden pipeline field preserves the stored fn + fn_args as JSON
    #     (MarkupSafe escapes `"` as &#34; inside the attribute value)
    assert "[{&#34;step_id&#34;: &#34;t1&#34;" in html
    assert "&#34;fn&#34;: &#34;Uppercase&#34;" in html
    assert "&#34;fn_args&#34;: {&#34;fmt&#34;: &#34;%Y-%m-%d&#34;}" in html
    # (3) the renderer binds a stored rule.fn back into the function <select>
    assert "fnSelectHtml(rule.fn||'')" in html
    assert "fnArgsCellHtml(rule)" in html
    # (4) the reader captures fn + fn_args when saving rules back
    assert "if(fnName) rule.fn = fnName;" in html
    assert "rule.fn_args = {fmt: val};" in html
    assert "rule.fn_args = {default: val};" in html
    # (5) the friendly per-function args UI is present
    assert "placeholder=\"%Y%m%d%H%M%S\"" in html
    assert "placeholder=\"value when source missing\"" in html