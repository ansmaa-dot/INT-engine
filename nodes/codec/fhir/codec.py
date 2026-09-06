"""FHIR R4 (JSON) codec.

Maps the canonical model onto the subset of FHIR R4 resources actually needed
by the laboratory/integration domain, wrapped in a Bundle:

  Patient         <- CanonicalMessage.patient
  ServiceRequest  <- CanonicalMessage.order
  Specimen        <- CanonicalMessage.specimen[]
  Observation     <- CanonicalMessage.observations[]

Clinician-relevant details that don't have a typed canonical field (e.g. HL7
abnormal flags) are carried on Observation.interpretation and restored into
Observation.extensions - never silently dropped.
"""
from __future__ import annotations

import json
from datetime import date, datetime
from uuid import uuid4

from core.errors import DecodeError, SerializeError
from core.model import (
    CanonicalMessage,
    Identifier,
    Observation,
    Order,
    OrderItem,
    PatientSummary,
    Specimen,
)
from core.wire import WireContext
from nodes.codec.base import Codec

_PRIO_TO_FHIR = {"routine": "routine", "stat": "stat", "asap": "asap", "preoperative": "urgent"}
_PRIO_FROM_FHIR = {"routine": "routine", "stat": "stat", "asap": "asap", "urgent": "preoperative"}

_STATUS_TO_FHIR = {
    "corrected": "corrected", "final": "final", "preliminary": "preliminary",
    "registered": "registered", "amended": "amended",
    "cancelled_with_results": "cancelled", "unable_to_obtain": "unknown",
    "deleted": "entered-in-error", "revised": "amended",
}
_STATUS_FROM_FHIR = {
    "corrected": "corrected", "final": "final", "preliminary": "preliminary",
    "registered": "registered", "amended": "amended", "cancelled": "cancelled_with_results",
    "unknown": "unable_to_obtain", "entered-in-error": "deleted",
}


def _parse_iso_date(s) -> date | None:
    try:
        return date.fromisoformat(s)
    except (TypeError, ValueError):
        return None


def _parse_iso_dt(s):
    if not s:
        return None
    try:
        return datetime.fromisoformat(s)
    except ValueError:
        return _parse_iso_date(s)


# --- identifier / coding helpers ---------------------------------------------

def _identifier(item: Identifier) -> dict:
    out = {"value": item.value}
    if item.system:
        out["system"] = item.system
    if item.type:
        out["type"] = {"coding": [{"code": item.type}]}
    return out


def _identifiers(items) -> list:
    return [_identifier(i) for i in items] or []


def _identifier_from(res: dict) -> Identifier:
    ident = Identifier(value=res.get("value"))
    if res.get("system"):
        ident.system = res["system"]
    typ = (res.get("type") or {}).get("coding") or []
    if typ and typ[0].get("code"):
        ident.type = typ[0]["code"]
    return ident


def _identifiers_from(items) -> list:
    return [_identifier_from(i) for i in (items or []) if isinstance(i, dict)]


def _coding(identifier: Identifier) -> dict:
    out = {}
    if identifier.value:
        out["code"] = identifier.value
    if identifier.system:
        out["system"] = identifier.system
    return {"coding": [out]} if out else {"coding": []}


def _coding_from(code) -> Identifier | None:
    if not isinstance(code, dict):
        return None
    coding = code.get("coding") or []
    if not coding:
        return None
    first = coding[0]
    return Identifier(value=first.get("code"), system=first.get("system"))


# --- canonical -> FHIR ------------------------------------------------------

def to_bundle(canonical: CanonicalMessage) -> dict:
    bundle = {
        "resourceType": "Bundle",
        "type": "collection",
        "id": canonical.metadata.message_id or str(uuid4()),
        "entry": [],
    }
    if canonical.patient is not None:
        bundle["entry"].append({"resource": _patient(canonical.patient)})
    if canonical.order is not None:
        bundle["entry"].append({"resource": _service_request(canonical.order)})
    for spec in canonical.specimen:
        bundle["entry"].append({"resource": _specimen(spec)})
    for obs in canonical.observations:
        bundle["entry"].append({"resource": _observation(obs)})
    return bundle


def _patient(patient: PatientSummary) -> dict:
    res = {"resourceType": "Patient"}
    ids = _identifiers(patient.identifiers)
    if ids:
        res["identifier"] = ids
    if patient.name:
        res["name"] = [{"text": patient.name}]
    if patient.dob is not None:
        res["birthDate"] = patient.dob.isoformat()
    if patient.gender:
        res["gender"] = patient.gender
    return res


def _service_request(order: Order) -> dict:
    res = {"resourceType": "ServiceRequest", "status": "active", "intent": "order"}
    ids = _identifiers(order.identifiers)
    if ids:
        res["identifier"] = ids
    if order.accession is not None:
        res["accessionNumber"] = order.accession.value
    if order.items and order.items[0].code is not None:
        res["code"] = _coding(order.items[0].code)
    if order.requested_at is not None:
        res["occurrenceDateTime"] = order.requested_at.isoformat()
    if order.priority:
        res["priority"] = _PRIO_TO_FHIR.get(order.priority, order.priority)
    if order.ordering_provider is not None:
        res["requester"] = {"identifier": _identifier(order.ordering_provider)}
    return res


def _specimen(spec: Specimen) -> dict:
    res = {"resourceType": "Specimen"}
    ids = _identifiers(spec.identifiers)
    if ids:
        res["identifier"] = ids
    if spec.type:
        res["type"] = {"text": spec.type}
    coll = {}
    if spec.collected_at is not None:
        coll["collectedDateTime"] = spec.collected_at.isoformat()
    if coll:
        res["collection"] = coll
    return res


def _observation(obs: Observation) -> dict:
    res = {"resourceType": "Observation"}
    res["status"] = _STATUS_TO_FHIR.get(obs.status or "final", obs.status or "final")
    if obs.code is not None:
        res["code"] = _coding(obs.code)
    if obs.value is not None:
        if isinstance(obs.value, (int, float)):
            q = {"value": obs.value}
            if obs.unit:
                q["unit"] = obs.unit
            res["valueQuantity"] = q
        else:
            res["valueString"] = str(obs.value)
    if obs.reference_range:
        res["referenceRange"] = [{"text": obs.reference_range}]
    if obs.observed_at is not None:
        res["effectiveDateTime"] = obs.observed_at.isoformat()
    flags = obs.extensions.get("abnormal_flags") or []
    if flags:
        res["interpretation"] = [{"coding": [{"code": f}]} for f in flags]
    return res


# --- FHIR -> canonical ------------------------------------------------------

def from_bundle(bundle, metadata=None) -> CanonicalMessage:
    if not isinstance(bundle, dict) or bundle.get("resourceType") != "Bundle":
        raise DecodeError("fhir.not_bundle", "FHIR payload is not a Bundle")
    canonical = CanonicalMessage()
    from core.model import MessageMetadata
    md = MessageMetadata(format="fhir-r4")
    md.message_id = bundle.get("id")
    if metadata is not None:
        md.received_at = metadata.received_at
    canonical.metadata = md

    for entry in bundle.get("entry") or []:
        res = entry.get("resource") if isinstance(entry, dict) else None
        if not isinstance(res, dict):
            continue
        rt = res.get("resourceType")
        if rt == "Patient":
            canonical.patient = _patient_from(res)
        elif rt == "ServiceRequest":
            canonical.order = _order_from(res)
        elif rt == "Specimen":
            canonical.specimen.append(_specimen_from(res))
        elif rt == "Observation":
            canonical.observations.append(_observation_from(res))
    return canonical


def _patient_from(res: dict) -> PatientSummary:
    p = PatientSummary()
    p.identifiers = _identifiers_from(res.get("identifier"))
    names = res.get("name") or []
    if names and isinstance(names[0], dict) and names[0].get("text"):
        p.name = names[0]["text"]
    p.dob = _parse_iso_date(res.get("birthDate"))
    p.gender = res.get("gender")
    return p


def _order_from(res: dict) -> Order:
    order = Order()
    order.identifiers = _identifiers_from(res.get("identifier"))
    acc = res.get("accessionNumber")
    if acc:
        order.accession = Identifier(value=acc, type="FILLER")
    code = _coding_from(res.get("code"))
    if code is not None:
        order.items = [OrderItem(code=code)]
    order.requested_at = _parse_iso_dt(res.get("occurrenceDateTime"))
    if res.get("priority"):
        order.priority = _PRIO_FROM_FHIR.get(res["priority"], res["priority"])
    req = res.get("requester") or {}
    if isinstance(req, dict) and isinstance(req.get("identifier"), dict):
        order.ordering_provider = _identifier_from(req["identifier"])
        order.ordering_provider.type = "PROVIDER"
    return order


def _specimen_from(res: dict) -> Specimen:
    spec = Specimen()
    spec.identifiers = _identifiers_from(res.get("identifier"))
    typ = res.get("type") or {}
    if isinstance(typ, dict):
        spec.type = typ.get("text") or ((typ.get("coding") or [{}])[0].get("code"))
    coll = res.get("collection") or {}
    if isinstance(coll, dict):
        spec.collected_at = _parse_iso_dt(coll.get("collectedDateTime"))
    return spec


def _observation_from(res: dict) -> Observation:
    obs = Observation()
    obs.status = _STATUS_FROM_FHIR.get(res.get("status"))
    obs.code = _coding_from(res.get("code"))
    vq = res.get("valueQuantity")
    vs = res.get("valueString")
    if isinstance(vq, dict):
        obs.value = vq.get("value")
        obs.unit = vq.get("unit")
    elif vs is not None:
        obs.value = vs
    rr = res.get("referenceRange") or []
    if rr and isinstance(rr[0], dict):
        obs.reference_range = rr[0].get("text")
    obs.observed_at = _parse_iso_dt(res.get("effectiveDateTime"))
    flags = []
    for interp in res.get("interpretation") or []:
        if isinstance(interp, dict) and (interp.get("coding") or []):
            flags.append(interp["coding"][0].get("code"))
    flags = [f for f in flags if f]
    if flags:
        obs.extensions = {"abnormal_flags": flags}
    return obs


class FhirR4Codec(Codec):
    """FHIR R4 JSON codec (Bundle of Patient/ServiceRequest/Specimen/
    Observation), translating between wire JSON and CanonicalMessage."""

    def __init__(self, key: str = "fhir.r4"):
        self.key = key

    def parse(self, raw, metadata: WireContext | None = None) -> CanonicalMessage:
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8", errors="replace")
        try:
            bundle = json.loads(raw)
        except (TypeError, ValueError) as e:
            raise DecodeError("fhir.invalid_json", f"malformed FHIR JSON: {e}", cause=e) from e
        return from_bundle(bundle, metadata)

    def serialize(self, canonical: CanonicalMessage) -> str:
        try:
            return json.dumps(to_bundle(canonical), sort_keys=True)
        except (TypeError, ValueError) as e:
            raise SerializeError("fhir.serialize", f"could not serialize canonical to FHIR: {e}", cause=e) from e
