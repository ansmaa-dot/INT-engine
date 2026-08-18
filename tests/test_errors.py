import pytest

from core.errors import (
    BusinessValidationError,
    CanonicalValidationError,
    DecodeError,
    DestinationError,
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