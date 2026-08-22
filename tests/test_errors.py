import pytest

from core.errors import (
    AssertError,
    BusinessValidationError,
    CanonicalValidationError,
    DecodeError,
    DestinationError,
    EnrichmentError,
    FilterError,
    PipelineError,
    SerializeError,
    TransformError,
    WireError,
)


@pytest.mark.parametrize(
    "cls,stage",
    [
        (WireError, "wire"),
        (DecodeError, "decode"),
        (CanonicalValidationError, "validation"),
        (BusinessValidationError, "business"),
        (TransformError, "transform"),
        (EnrichmentError, "enrichment"),
        (FilterError, "filter"),
        (AssertError, "assert"),
        (SerializeError, "serialize"),
        (DestinationError, "destination"),
    ],
)
def test_each_stage_error_shape(cls, stage):
    e = cls("some.code", "boom", context={"trace_id": "t1"})
    assert isinstance(e, PipelineError)
    assert e.stage == stage
    assert e.code == "some.code"
    assert e.context == {"trace_id": "t1"}
    # stable code visible in the message representation
    assert "some.code" in str(e)


def test_all_errors_share_stable_base_interface():
    for cls in (
        WireError,
        DecodeError,
        CanonicalValidationError,
        BusinessValidationError,
        TransformError,
        EnrichmentError,
        FilterError,
        AssertError,
        SerializeError,
        DestinationError,
    ):
        assert issubclass(cls, PipelineError)
        assert cls.stage


def test_cause_context_is_retained():
    inner = ValueError("root cause")
    e = DecodeError("json.invalid", "malformed JSON", cause=inner)
    # the cause is retained as an attribute for diagnostics. Actual
    # ``__cause__`` chaining is done by callers via ``raise ... from``
    # (the json codec does this), which the ``cause`` kwarg mirrors.
    assert e.cause is inner


def test_context_is_copied_not_mutated_shared():
    shared = {"k": 1}
    e = WireError("x", "y", context=shared)
    shared["k"] = 99
    assert e.context == {"k": 1}


# --- P0: step identity, retryability, FilterError/AssertError ----------


def test_step_identity_attributes_set_when_provided():
    e = TransformError(
        "map.bad", "mapping failed",
        step_id="abc-123", step_type="transform"
    )
    assert e.step_id == "abc-123"
    assert e.step_type == "transform"


def test_step_identity_defaults_to_none():
    e = TransformError("map.bad", "mapping failed")
    assert e.step_id is None
    assert e.step_type is None
    assert e.retryable is None


def test_step_identity_merged_into_context():
    e = TransformError(
        "map.bad", "mapping failed",
        step_id="abc-123", step_type="transform",
        context={"trace_id": "t1"}
    )
    assert e.context == {
        "trace_id": "t1",
        "step_id": "abc-123",
        "step_type": "transform",
    }


def test_step_identity_does_not_overwrite_existing_context_keys():
    """When context already has step_id, the step kwarg does not overwrite."""
    e = TransformError(
        "map.bad", "mapping failed",
        step_id="abc-kwarg", step_type="transform",
        context={"step_id": "abc-existing", "trace_id": "t1"}
    )
    assert e.context["step_id"] == "abc-existing"
    assert e.context["step_type"] == "transform"


def test_retryable_is_stored():
    e = DestinationError("dest.fail", "boom", retryable=True)
    assert e.retryable is True

    e2 = DestinationError("dest.fail", "boom", retryable=False)
    assert e2.retryable is False

    e3 = DestinationError("dest.fail", "boom")
    assert e3.retryable is None


def test_str_representation_unchanged_with_new_kwargs():
    """str(exc) must not change format with step_id/step_type/retryable."""
    e = TransformError(
        "map.bad", "mapping failed",
        step_id="abc-123", step_type="transform", retryable=False
    )
    s = str(e)
    assert s.startswith("[transform/map.bad]")
    assert "mapping failed" in s
    # step_id/step_type/retryable are NOT in str representation
    assert "abc-123" not in s


def test_filter_error_has_correct_stage():
    e = FilterError("filter.expr", "condition false")
    assert e.stage == "filter"
    assert isinstance(e, PipelineError)


def test_assert_error_has_correct_stage():
    e = AssertError("assert.fail", "assertion failed")
    assert e.stage == "assert"
    assert isinstance(e, PipelineError)


def test_filter_error_is_always_pipeline_error():
    e = FilterError("f.code", "msg")
    assert isinstance(e, PipelineError)


def test_assert_error_is_always_pipeline_error():
    e = AssertError("a.code", "msg")
    assert isinstance(e, PipelineError)