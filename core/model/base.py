"""Canonical healthcare/laboratory message model.

Every wire format (HL7 v2, FHIR, XML, CSV, ...) is decoded *into* this
representation, and every outbound format is serialized *from* it. The rest
of the engine only ever deals with this model, never a raw wire format.

Deliberately small and focused on the laboratory/integration domain
(patient, encounter, order, specimen, observations) plus identifiers,
timestamps, metadata, and a documented escape hatch for vendor-specific
data. We are NOT trying to model all of FHIR or the whole healthcare domain
here.

Design rules:
  * Fields are optional by default — wire formats are lossy, so a codec
    fills in only what the source message actually expressed.
  * Domain concepts carry typed ``Identifier`` objects (system+value+type)
    rather than bare strings, so cross-format transforms stay lossless.
  * Anything that doesn't fit the typed fields goes into ``extensions`` —
    an explicit escape hatch — never inserted ad hoc into typed fields.
  * No pipeline runtime state (retries, queue state, DLQ, cancellation)
    lives here. See ``MessageMetadata``/``WireContext`` for provenance and
    transport context, and the queue/Envelope for lifecycle state.
"""
from __future__ import annotations

from datetime import date, datetime
from typing import Any, Literal

from pydantic import BaseModel, Field


class Identifier(BaseModel):
    """A typed external identifier (MRN, SSN, accession, LOINC, ...).

    ``value`` is required; ``system``/``type`` give it meaning and are
    optional because some wire formats don't carry them.
    """
    system: str | None = None      # namespace/OID/URI that defines ``value``
    value: str
    type: str | None = None        # e.g. "MRN", "SSN", "accession", "placer"


class PatientSummary(BaseModel):
    identifiers: list[Identifier] = Field(default_factory=list)
    name: str | None = None
    dob: date | None = None
    gender: str | None = None


class Encounter(BaseModel):
    identifiers: list[Identifier] = Field(default_factory=list)
    visit_number: Identifier | None = None
    started_at: datetime | None = None


class OrderItem(BaseModel):
    """One ordered test / request line (e.g. a LOINC-coded request)."""
    identifiers: list[Identifier] = Field(default_factory=list)
    code: Identifier | None = None
    requested_at: datetime | None = None
    priority: str | None = None


class Order(BaseModel):
    identifiers: list[Identifier] = Field(default_factory=list)
    accession: Identifier | None = None
    requested_at: datetime | None = None
    priority: str | None = None
    ordering_provider: Identifier | None = None
    items: list[OrderItem] = Field(default_factory=list)


class Specimen(BaseModel):
    identifiers: list[Identifier] = Field(default_factory=list)
    type: str | None = None
    collected_at: datetime | None = None


class Observation(BaseModel):
    """A single result/observation. ``value`` is a scalar because lab results
    are typically numeric, textual, or boolean; coded values would go in the
    ``code`` ``Identifier``.

    ``extensions`` preserves clinician-relevant details that codecs surface
    but that don't belong in the typed core fields (e.g. HL7 OBX-10 abnormal
    flags, specimen status) — so they are never silently discarded on a
    parse/serialize round trip.
    """
    identifiers: list[Identifier] = Field(default_factory=list)
    code: Identifier | None = None     # e.g. LOINC
    status: str | None = None
    value: str | int | float | bool | None = None
    unit: str | None = None
    reference_range: str | None = None
    observed_at: datetime | None = None
    extensions: dict[str, Any] = Field(default_factory=dict)


class MessageMetadata(BaseModel):
    """Provenance describing how the canonical message was produced.

    This is domain-adjacent provenance (format/version/profile, source,
    message id, receive time) — NOT pipeline runtime state. Retry/DLQ/queue
    state is deliberately excluded and lives on the Envelope/queue layer
    (added in Pass 2), not here.
    """
    format: str | None = None
    version: str | None = None
    profile: str | None = None
    message_type: str | None = None
    source: str | None = None
    message_id: str | None = None
    received_at: datetime | None = None


class CanonicalMessage(BaseModel):
    """Top-level normalized message — the only currency between Decode and
    Encode in the pipeline.

    ``schema_version`` pins the model contract so a future breaking change
    becomes an explicit ``1.1`` that codecs opt into, rather than a silent
    drift.
    """
    schema_version: Literal["1.0"] = "1.0"

    patient: PatientSummary | None = None
    encounter: Encounter | None = None
    order: Order | None = None
    specimen: list[Specimen] = Field(default_factory=list)
    observations: list[Observation] = Field(default_factory=list)

    metadata: MessageMetadata = Field(default_factory=MessageMetadata)
    extensions: dict[str, Any] = Field(default_factory=dict)
