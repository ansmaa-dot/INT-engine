"""Static field catalog for the canonical model.

Mirrors ``FIELD_MAPPING.md`` as a single source of truth for field paths,
human labels, types, and per-codec source hints.

Design constraints (see ``docs/ui_field_picker.md`` §9):
- ``Identifier`` fields expose ``.value`` / ``.system`` / ``.type`` leaves.
- List entries use example index ``0``.
- ``lookups.*`` is the enrichment namespace (dynamic; no static entries).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

# ---------------------------------------------------------------------------
# Types
# ---------------------------------------------------------------------------
FieldKind = Literal[
    "str", "datetime",
    "identifier_value", "identifier_system", "identifier_type",
    "list",
]
FieldGroup = Literal[
    "patient", "encounter", "order", "specimen",
    "observations", "metadata", "extensions", "lookups",
]


@dataclass(frozen=True)
class FieldDescriptor:
    path: str                                           # dotted canonical path (leaf)
    label: str                                          # human-friendly name
    kind: FieldKind
    group: FieldGroup
    codec_hints: dict[str, str] = field(default_factory=dict)
# ===================================================================
# Master catalog — one entry per leaf-addressable canonical field.
# ===================================================================

FIELD_CATALOG: list[FieldDescriptor] = [
    # ── patient ──────────────────────────────────────────────────
    FieldDescriptor("patient.name", "Patient Name", "str", "patient",
                    {"hl7v2.5.1.ORU_R01": "PID-5",
                     "hl7v2.5.1.ADT_A01": "PID-5",
                     "hl7v2.5.1.ORM_O01": "PID-5",
                     "fhir.r4": "Patient.name.text"}),
    FieldDescriptor("patient.dob", "Date of Birth", "datetime", "patient",
                    {"hl7v2.5.1.ORU_R01": "PID-7",
                     "hl7v2.5.1.ADT_A01": "PID-7",
                     "fhir.r4": "Patient.birthDate"}),
    FieldDescriptor("patient.gender", "Gender", "str", "patient",
                    {"hl7v2.5.1.ORU_R01": "PID-8",
                     "fhir.r4": "Patient.gender"}),
    FieldDescriptor("patient.identifiers.0.value", "Patient ID Value", "identifier_value", "patient",
                    {"hl7v2.5.1.ORU_R01": "PID-3.1",
                     "fhir.r4": "Patient.identifier[].value"}),
    FieldDescriptor("patient.identifiers.0.system", "Patient ID System", "identifier_system", "patient",
                    {"hl7v2.5.1.ORU_R01": "PID-3.4",
                     "fhir.r4": "Patient.identifier[].system"}),
    FieldDescriptor("patient.identifiers.0.type", "Patient ID Type", "identifier_type", "patient",
                    {"hl7v2.5.1.ORU_R01": "PID-3.5",
                     "fhir.r4": "Patient.identifier[].type.coding[].code"}),

    # ── encounter ────────────────────────────────────────────────
    FieldDescriptor("encounter.visit_number.value", "Visit Number", "identifier_value", "encounter",
                    {"hl7v2.5.1.ORU_R01": "PV1-19",
                     "hl7v2.5.1.ADT_A01": "PV1-19",
                     "fhir.r4": "Encounter.identifier[].value"}),
    FieldDescriptor("encounter.visit_number.system", "Visit Number System", "identifier_system", "encounter",
                    {"fhir.r4": "Encounter.identifier[].system"}),
    FieldDescriptor("encounter.visit_number.type", "Visit Number Type", "identifier_type", "encounter"),
    FieldDescriptor("encounter.started_at", "Encounter Start", "datetime", "encounter",
                    {"fhir.r4": "Encounter.period.start"}),
    FieldDescriptor("encounter.identifiers.0.value", "Encounter ID Value", "identifier_value", "encounter"),
    FieldDescriptor("encounter.identifiers.0.system", "Encounter ID System", "identifier_system", "encounter"),
    FieldDescriptor("encounter.identifiers.0.type", "Encounter ID Type", "identifier_type", "encounter"),
# ── order ────────────────────────────────────────────────────
    FieldDescriptor("order.accession.value", "Accession Number", "identifier_value", "order",
                    {"hl7v2.5.1.ORU_R01": "ORC-3 / OBR-3",
                     "hl7v2.5.1.ORM_O01": "ORC-3 / OBR-3",
                     "fhir.r4": "ServiceRequest.identifier[].value"}),
    FieldDescriptor("order.accession.system", "Accession System", "identifier_system", "order"),
    FieldDescriptor("order.accession.type", "Accession Type", "identifier_type", "order"),
    FieldDescriptor("order.requested_at", "Order Requested At", "datetime", "order",
                    {"hl7v2.5.1.ORU_R01": "OBR-7",
                     "hl7v2.5.1.ORM_O01": "ORC-9 / OBR-7",
                     "fhir.r4": "ServiceRequest.authoredOn"}),
    FieldDescriptor("order.priority", "Order Priority", "str", "order",
                    {"hl7v2.5.1.ORU_R01": "OBR-27",
                     "fhir.r4": "ServiceRequest.priority"}),
    FieldDescriptor("order.ordering_provider.value", "Ordering Provider", "identifier_value", "order",
                    {"hl7v2.5.1.ORU_R01": "OBR-16",
                     "fhir.r4": "ServiceRequest.requester.identifier.value"}),
    FieldDescriptor("order.ordering_provider.system", "Ordering Provider System", "identifier_system", "order"),
    FieldDescriptor("order.ordering_provider.type", "Ordering Provider Type", "identifier_type", "order"),
    FieldDescriptor("order.identifiers.0.value", "Order ID Value", "identifier_value", "order",
                    {"hl7v2.5.1.ORU_R01": "ORC-2 / OBR-2",
                     "fhir.r4": "ServiceRequest.identifier[].value"}),
    FieldDescriptor("order.identifiers.0.system", "Order ID System", "identifier_system", "order"),
    FieldDescriptor("order.identifiers.0.type", "Order ID Type", "identifier_type", "order"),

    # ── order.items[] ────────────────────────────────────────────
    FieldDescriptor("order.items.0.code.value", "Ordered Test Code", "identifier_value", "order",
                    {"hl7v2.5.1.ORU_R01": "OBR-4.1",
                     "hl7v2.5.1.ORM_O01": "OBR-4.1",
                     "fhir.r4": "ServiceRequest.code.coding[].code"}),
    FieldDescriptor("order.items.0.code.system", "Ordered Test Code System", "identifier_system", "order"),
    FieldDescriptor("order.items.0.code.type", "Ordered Test Code Type", "identifier_type", "order"),
    FieldDescriptor("order.items.0.requested_at", "Ordered Test Requested At", "datetime", "order"),
    FieldDescriptor("order.items.0.priority", "Ordered Test Priority", "str", "order"),
    FieldDescriptor("order.items.0.identifiers.0.value", "Ordered Test ID Value", "identifier_value", "order"),
    FieldDescriptor("order.items.0.identifiers.0.system", "Ordered Test ID System", "identifier_system", "order"),
    FieldDescriptor("order.items.0.identifiers.0.type", "Ordered Test ID Type", "identifier_type", "order"),

    # ── specimen[] ───────────────────────────────────────────────
    FieldDescriptor("specimen.0.type", "Specimen Type", "str", "specimen",
                    {"hl7v2.5.1.ORU_R01": "SPM-4",
                     "fhir.r4": "Specimen.type.text"}),
    FieldDescriptor("specimen.0.collected_at", "Specimen Collected At", "datetime", "specimen",
                    {"hl7v2.5.1.ORU_R01": "SPM-17",
                     "fhir.r4": "Specimen.collection.collectedDateTime"}),
    FieldDescriptor("specimen.0.identifiers.0.value", "Specimen ID Value", "identifier_value", "specimen",
                    {"hl7v2.5.1.ORU_R01": "SPM-2",
                     "fhir.r4": "Specimen.identifier[].value"}),
    FieldDescriptor("specimen.0.identifiers.0.system", "Specimen ID System", "identifier_system", "specimen"),
    FieldDescriptor("specimen.0.identifiers.0.type", "Specimen ID Type", "identifier_type", "specimen"),
# ── observations[] ───────────────────────────────────────────
    FieldDescriptor("observations.0.value", "Result Value", "str", "observations",
                    {"hl7v2.5.1.ORU_R01": "OBX-5",
                     "fhir.r4": "Observation.valueQuantity.value / valueString"}),
    FieldDescriptor("observations.0.unit", "Result Unit", "str", "observations",
                    {"hl7v2.5.1.ORU_R01": "OBX-6",
                     "fhir.r4": "Observation.valueQuantity.unit"}),
    FieldDescriptor("observations.0.reference_range", "Reference Range", "str", "observations",
                    {"hl7v2.5.1.ORU_R01": "OBX-7",
                     "fhir.r4": "Observation.referenceRange[].text"}),
    FieldDescriptor("observations.0.status", "Observation Status", "str", "observations",
                    {"hl7v2.5.1.ORU_R01": "OBX-11",
                     "fhir.r4": "Observation.status"}),
    FieldDescriptor("observations.0.observed_at", "Observation Time", "datetime", "observations",
                    {"hl7v2.5.1.ORU_R01": "OBX-14",
                     "fhir.r4": "Observation.effectiveDateTime"}),
    FieldDescriptor("observations.0.code.value", "Observation Code", "identifier_value", "observations",
                    {"hl7v2.5.1.ORU_R01": "OBX-3.1",
                     "fhir.r4": "Observation.code.coding[].code"}),
    FieldDescriptor("observations.0.code.system", "Observation Code System", "identifier_system", "observations"),
    FieldDescriptor("observations.0.code.type", "Observation Code Type", "identifier_type", "observations"),
    FieldDescriptor("observations.0.identifiers.0.value", "Observation ID Value", "identifier_value", "observations"),
    FieldDescriptor("observations.0.identifiers.0.system", "Observation ID System", "identifier_system", "observations"),
    FieldDescriptor("observations.0.identifiers.0.type", "Observation ID Type", "identifier_type", "observations"),

    # ── metadata ─────────────────────────────────────────────────
    FieldDescriptor("metadata.format", "Source Format", "str", "metadata"),
    FieldDescriptor("metadata.version", "Source Version", "str", "metadata",
                    {"hl7v2.5.1.ORU_R01": "MSH-12",
                     "fhir.r4": "Bundle.meta.lastUpdated"}),
    FieldDescriptor("metadata.profile", "Message Profile", "str", "metadata",
                    {"hl7v2.5.1.ORU_R01": "MSH-9.1",
                     "fhir.r4": "Bundle.meta.profile"}),
    FieldDescriptor("metadata.message_type", "Message Type", "str", "metadata",
                    {"hl7v2.5.1.ORU_R01": "MSH-9",
                     "fhir.r4": "Bundle.type"}),
    FieldDescriptor("metadata.source", "Source Application", "str", "metadata",
                    {"hl7v2.5.1.ORU_R01": "MSH-3",
                     "fhir.r4": "MessageHeader.source.name"}),
    FieldDescriptor("metadata.message_id", "Message ID", "str", "metadata",
                    {"hl7v2.5.1.ORU_R01": "MSH-10",
                     "fhir.r4": "Bundle.id"}),
    FieldDescriptor("metadata.received_at", "Received At", "datetime", "metadata"),

    # ── extensions (top-level vendor data) ───────────────────────
    FieldDescriptor("extensions", "Extensions", "str", "extensions"),
]
# ===================================================================
# Derived lookup maps (built once at import time)
# ===================================================================

_PATH_TO_DESCRIPTOR: dict[str, FieldDescriptor] = {d.path: d for d in FIELD_CATALOG}
_LABEL_TO_PATH: dict[str, str] = {d.label: d.path for d in FIELD_CATALOG}
_ALL_PATHS: frozenset[str] = frozenset(d.path for d in FIELD_CATALOG)

# ===================================================================
# Public API
# ===================================================================

def catalog_for_codec(codec_key: str | None = None) -> list[FieldDescriptor]:
    """Return the canonical field catalog, with optional per-codec hints.

    When ``codec_key`` is provided, descriptors carry source-field hints
    for that key (via a transient ``_hint`` attribute).  Codec key must
    match ``nodes.codec.registry`` exactly.

    ``lookups.*`` is dynamic (enrichment namespace) and never appears in
    the static catalog.
    """
    if codec_key is None:
        return list(FIELD_CATALOG)

    result: list[FieldDescriptor] = []
    for d in FIELD_CATALOG:
        hint = d.codec_hints.get(codec_key)
        # Create a transient wrapper with _hint set. FieldDescriptor is
        # frozen so we use object.__setattr__ for UI consumption only.
        obj = object.__new__(FieldDescriptor)
        object.__setattr__(obj, "path", d.path)
        object.__setattr__(obj, "label", d.label)
        object.__setattr__(obj, "kind", d.kind)
        object.__setattr__(obj, "group", d.group)
        object.__setattr__(obj, "codec_hints", d.codec_hints)
        object.__setattr__(obj, "_hint", hint)
        result.append(obj)
    return result


def validate_canonical_path(path: str) -> bool:
    """Return ``True`` if *path* is a known leaf in the static catalog.

    Handles alternate list indices by canonicalizing numeric segments to
    ``0`` before lookup.
    """
    if path in _ALL_PATHS:
        return True
    parts = path.split(".")
    canon = []
    for p in parts:
        if p.isdigit() or (p.startswith("-") and p[1:].isdigit()):
            canon.append("0")
        else:
            canon.append(p)
    return ".".join(canon) in _ALL_PATHS


def all_catalog_paths() -> frozenset[str]:
    """All known canonical leaf paths (for static expression/rule validation)."""
    return _ALL_PATHS


def identifier_object_paths() -> frozenset[str]:
    """Container paths of Identifier objects, e.g. ``patient.identifiers.0``.

    Derived from the ``identifier_*`` catalog leaves by stripping the trailing
    ``.value`` / ``.system`` / ``.type`` segment. A transform target that
    addresses one of these would assign a whole object to an Identifier leaf
    — rejected at config-save time (fail fast).
    """
    return frozenset(
        d.path.rsplit(".", 1)[0]
        for d in FIELD_CATALOG
        if d.kind in ("identifier_value", "identifier_system", "identifier_type")
    )


def human_label(path: str) -> str | None:
    """Return the human-readable label for a canonical path, or None."""
    # Try exact match, then index-canonicalized
    if path in _PATH_TO_DESCRIPTOR:
        return _PATH_TO_DESCRIPTOR[path].label
    parts = path.split(".")
    canon = []
    for p in parts:
        if p.isdigit() or (p.startswith("-") and p[1:].isdigit()):
            canon.append("0")
        else:
            canon.append(p)
    key = ".".join(canon)
    d = _PATH_TO_DESCRIPTOR.get(key)
    return d.label if d else None


def canonical_path(label: str) -> str | None:
    """Return the canonical dotted path for a human-readable label."""
    return _LABEL_TO_PATH.get(label)