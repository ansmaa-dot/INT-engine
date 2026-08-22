"""Tests for the field catalog (core/field_catalog.py)."""
import pytest

from core.field_catalog import (
    FIELD_CATALOG,
    FieldDescriptor,
    _ALL_PATHS,
    _LABEL_TO_PATH,
    _PATH_TO_DESCRIPTOR,
    canonical_path,
    catalog_for_codec,
    human_label,
    validate_canonical_path,
)


# ── catalog structure ──────────────────────────────────────────

def test_catalog_is_non_empty():
    assert len(FIELD_CATALOG) > 0


def test_every_entry_is_field_descriptor():
    for d in FIELD_CATALOG:
        assert isinstance(d, FieldDescriptor)


def test_every_entry_has_path_label_kind_group():
    for d in FIELD_CATALOG:
        assert d.path, f"empty path on {d}"
        assert d.label, f"empty label on {d.path}"
        assert d.kind in (
            "str", "datetime",
            "identifier_value", "identifier_system", "identifier_type",
            "list",
        ), f"bad kind {d.kind!r} on {d.path}"
        assert d.group in (
            "patient", "encounter", "order", "specimen",
            "observations", "metadata", "extensions", "lookups",
        ), f"bad group {d.group!r} on {d.path}"


def test_catalog_has_essential_paths():
    """Smoke-test key paths from FIELD_MAPPING.md are present."""
    essential = [
        "patient.name",
        "patient.identifiers.0.value",
        "patient.dob",
        "encounter.visit_number.value",
        "order.accession.value",
        "order.ordering_provider.value",
        "order.items.0.code.value",
        "specimen.0.type",
        "observations.0.value",
        "observations.0.code.value",
        "metadata.message_type",
        "metadata.message_id",
    ]
    paths = {d.path for d in FIELD_CATALOG}
    for p in essential:
        assert p in paths, f"missing essential path: {p}"
# ── label ↔ path round-trips ───────────────────────────────────

def test_label_to_path_round_trip():
    """Every label maps to a path that maps back to the same label."""
    for d in FIELD_CATALOG:
        got_path = canonical_path(d.label)
        assert got_path is not None, f"canonical_path({d.label!r}) = None"
        got_label = human_label(got_path)
        assert got_label == d.label, (
            f"round-trip broken: {d.label!r} → {got_path} → {got_label!r}"
        )


def test_path_to_label_all_known():
    for d in FIELD_CATALOG:
        assert human_label(d.path) == d.label


# ── validate_canonical_path ────────────────────────────────────

def test_validate_known_path():
    assert validate_canonical_path("patient.name") is True


def test_validate_unknown_path():
    assert validate_canonical_path("patient.nonexistent") is False


def test_validate_alternate_list_index():
    """A path with index 3 should be accepted when catalog has index 0."""
    assert validate_canonical_path("patient.identifiers.3.value") is True
    assert validate_canonical_path("observations.5.code.value") is True


# ── human_label ────────────────────────────────────────────────

def test_human_label_unknown_path():
    assert human_label("nonexistent.path") is None


def test_human_label_with_alternate_index():
    assert human_label("patient.identifiers.7.value") == "Patient ID Value"


# ── catalog_for_codec ──────────────────────────────────────────

def test_catalog_for_codec_no_key_returns_all():
    assert len(catalog_for_codec()) == len(FIELD_CATALOG)


def test_catalog_for_codec_hl7_has_hints():
    cat = catalog_for_codec("hl7v2.5.1.ORU_R01")
    names = [d for d in cat if d.path == "patient.name"]
    assert len(names) == 1
    assert hasattr(names[0], "_hint")
    assert names[0]._hint == "PID-5"  # type: ignore[attr-defined]


def test_catalog_for_codec_fhir_has_hints():
    cat = catalog_for_codec("fhir.r4")
    names = [d for d in cat if d.path == "patient.name"]
    assert len(names) == 1
    assert names[0]._hint == "Patient.name.text"  # type: ignore[attr-defined]


def test_catalog_for_codec_missing_codec_has_no_hints():
    cat = catalog_for_codec("nonexistent.codec")
    assert len(cat) == len(FIELD_CATALOG)
    for d in cat:
        assert d._hint is None  # type: ignore[attr-defined]
# ── Identifier granularity ─────────────────────────────────────

def test_identifier_fields_have_value_system_type_leaves():
    """Verify Identifier-bearing fields expose .value/.system/.type leaves."""
    id_leaves = {
        "patient.identifiers.0.value",
        "patient.identifiers.0.system",
        "patient.identifiers.0.type",
        "encounter.visit_number.value",
        "encounter.visit_number.system",
        "encounter.visit_number.type",
        "order.accession.value",
        "order.accession.system",
        "order.accession.type",
        "order.ordering_provider.value",
        "order.ordering_provider.system",
        "order.ordering_provider.type",
        "order.items.0.code.value",
        "order.items.0.code.system",
        "order.items.0.code.type",
        "observations.0.code.value",
        "observations.0.code.system",
        "observations.0.code.type",
    }
    paths = {d.path for d in FIELD_CATALOG}
    for p in id_leaves:
        assert p in paths, f"missing Identifier leaf: {p}"


def test_no_whole_identifier_object_in_catalog():
    """Catalog must NOT have bare Identifier objects.
    
    Users must address .value/.system/.type — assigning a whole object
    would fail model validation.
    """
    paths = {d.path for d in FIELD_CATALOG}
    bad = [
        "order.accession",
        "order.ordering_provider",
        "encounter.visit_number",
        "observations.0.code",
        "order.items.0.code",
    ]
    for p in bad:
        assert p not in paths, f"whole Identifier {p!r} should not be in catalog"


# ── Derived maps ───────────────────────────────────────────────

def test_derived_maps_cover_all():
    assert len(_PATH_TO_DESCRIPTOR) == len(FIELD_CATALOG)
    assert len(_LABEL_TO_PATH) == len(FIELD_CATALOG)
    assert len(_ALL_PATHS) == len(FIELD_CATALOG)