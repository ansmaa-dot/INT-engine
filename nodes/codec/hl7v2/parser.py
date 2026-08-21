"""HL7 v2 → CanonicalMessage parsing for the laboratory profiles (ORU_R01,
ADT_A01, v2.5.1). Segment structure, delimiters, repetitions, components and
HL7 escaping are all handled here — inside the codec, never the transport.
"""
from __future__ import annotations

from datetime import datetime

from core.errors import DecodeError
from core.model import (
    CanonicalMessage,
    Encounter,
    Identifier,
    MessageMetadata,
    Observation,
    Order,
    OrderItem,
    PatientSummary,
    Specimen,
)
from core.wire import WireContext
from nodes.codec.hl7v2.er7 import (
    HL7Error,
    component,
    parse_message,
    split_subcomponents,
    unescape,
)

_LOINC_SYSTEMS = {"LN", "L", "LOINC"}

_OBX_STATUS = {
    "C": "corrected", "F": "final", "P": "preliminary", "R": "registered",
    "A": "amended", "X": "cancelled_with_results", "U": "unable_to_obtain",
    "D": "deleted", "S": "revised",
}
_OBX_STATUS_INV = {v: k for k, v in _OBX_STATUS.items()}

_PRIORITY = {"R": "routine", "S": "stat", "A": "asap", "P": "preoperative"}
_PRIORITY_INV = {v: k for k, v in _PRIORITY.items()}


def _coding_system(code: str) -> str | None:
    return "http://loinc.org" if code in _LOINC_SYSTEMS else (code or None)


def _coerce_datetime(raw) -> datetime | None:
    if not raw:
        return None
    s = str(raw).strip()
    for fmt, n in (("%Y%m%d%H%M%S", 14), ("%Y%m%d%H%M", 12), ("%Y%m%d", 8)):
        try:
            return datetime.strptime(s[:n], fmt)
        except ValueError:
            continue
    return None


def _coerce_value(raw) -> str | int | float:
    s = str(raw).strip()
    try:
        f = float(s)
        return int(f) if f.is_integer() else f
    except ValueError:
        return s


def _identifier_from_cx(value) -> Identifier | None:
    """PID-3 style CX: id ^ check ^ scheme ^ assigning_authority ^ type."""
    if not value:
        return None
    num = component(value, 0)
    if not num:
        return None
    return Identifier(
        value=unescape(num),
        system=unescape(component(value, 3)) or None,
        type=unescape(component(value, 4)) or None,
    )


def _identifier_from_ei(value, idtype: str) -> Identifier | None:
    """OBR/ORC style EI: entity_id ^ namespace_id."""
    if not value:
        return None
    num = component(value, 0)
    if not num:
        return None
    return Identifier(
        value=unescape(num),
        system=unescape(component(value, 1)) or None,
        type=idtype,
    )


def _identifier_from_xcn(value, idtype: str) -> Identifier | None:
    if not value:
        return None
    v = unescape(component(value, 0))
    if not v:
        return None
    return Identifier(value=v, type=idtype)


def parse_to_canonical(raw: str, metadata: WireContext | None = None) -> CanonicalMessage:
    try:
        msg = parse_message(raw)
    except HL7Error as e:
        raise DecodeError("hl7.malformed", f"malformed HL7 message: {e}", cause=e) from e

    msh = msg.first("MSH")
    if msh is None:
        raise DecodeError("hl7.no_msh", "HL7 message has no MSH segment")
    canonical = CanonicalMessage()
    metadata_model = MessageMetadata(format="hl7v2")
    metadata_model.version = unescape(msh.get(11)) or "2.5.1"            # MSH-12
    metadata_model.message_type = unescape(msh.get(8)) or None           # MSH-9 msgtype
    metadata_model.message_id = unescape(msh.get(9)) or None                   # MSH-10
    metadata_model.source = unescape(msh.get(2)) or None                       # MSH-3
    if metadata is not None:
        metadata_model.received_at = metadata.received_at
    canonical.metadata = metadata_model

    _parse_patient(msg, canonical)
    _parse_encounter(msg, canonical)
    _parse_order(msg, canonical)
    _parse_specimens(msg, canonical)
    _parse_observations(msg, canonical)

    return canonical


def _parse_patient(msg, canonical: CanonicalMessage) -> None:
    pid = msg.first("PID")
    if pid is None:
        return
    patient = PatientSummary()
    for rep in str(pid.get(3) or "").split("~"):  # PID-3
        ident = _identifier_from_cx(rep)
        if ident is not None:
            patient.identifiers.append(ident)

    name = _patient_name(pid.get(5))  # PID-5
    if name:
        patient.name = name
    _dt = _coerce_datetime(unescape(pid.get(7)))  # PID-7
    patient.dob = _dt.date() if _dt is not None else None
    patient.gender = unescape(pid.get(8)) or None          # PID-8
    canonical.patient = patient


def _patient_name(pid5) -> str | None:
    if not pid5:
        return None
    family = unescape(component(pid5, 0))
    given = unescape(component(pid5, 1))
    if family and given:
        return f"{given} {family}"
    return family or given or None


def _parse_encounter(msg, canonical: CanonicalMessage) -> None:
    pv1 = msg.first("PV1")
    if pv1 is None:
        return
    enc = Encounter()
    visit = _identifier_from_cx(pv1.get(19))  # PV1-19 visit number
    if visit is not None:
        enc.visit_number = visit
    canonical.encounter = enc
def _parse_order(msg, canonical: CanonicalMessage) -> None:
    obr_obj = msg.first("OBR")
    orc_obj = msg.first("ORC")
    if obr_obj is None and orc_obj is None:
        return
    order = Order()

    if orc_obj is not None:
        placer = _identifier_from_ei(orc_obj.get(2), "PLACER")  # ORC-2
        filler = _identifier_from_ei(orc_obj.get(3), "FILLER")  # ORC-3
        if placer is not None:
            order.identifiers.append(placer)
        if filler is not None:
            order.identifiers.append(filler)
        if not order.accession and filler is not None:
            order.accession = filler

    if obr_obj is not None:
        placer2 = _identifier_from_ei(obr_obj.get(2), "PLACER")  # OBR-2
        filler2 = _identifier_from_ei(obr_obj.get(3), "FILLER")  # OBR-3
        if filler2 is not None:
            order.accession = filler2
            if not any(i.type == "FILLER" for i in order.identifiers):
                order.identifiers.append(filler2)
        if placer2 is not None and not any(i.type == "PLACER" for i in order.identifiers):
            order.identifiers.append(placer2)
        order.requested_at = _coerce_datetime(unescape(component(obr_obj.get(7), 0)))  # OBR-7
        priority = unescape(component(obr_obj.get(27), 0))                             # OBR-27
        order.priority = _PRIORITY.get(priority, priority or None)
        provider = _identifier_from_xcn(obr_obj.get(16), "PROVIDER")                   # OBR-16
        if provider is not None and provider.value:
            order.ordering_provider = provider
        code = _test_code(obr_obj.get(4))  # OBR-4
        if code is not None:
            order.items = [OrderItem(code=code)]

    canonical.order = order


def _test_code(ce) -> Identifier | None:
    if not ce:
        return None
    code = unescape(component(ce, 0))
    if not code:
        return None
    return Identifier(
        value=code,
        system=_coding_system(unescape(component(ce, 2))),
    )


def _parse_specimens(msg, canonical: CanonicalMessage) -> None:
    for spm in msg.all("SPM"):
        spec = Specimen()
        spm_id = unescape(component(spm.get(2), 0))  # SPM-2 specimen id
        if spm_id:
            spec.identifiers.append(Identifier(value=spm_id, type="SPECIMEN"))
        type_code = unescape(component(spm.get(4), 0))  # SPM-4
        type_text = unescape(component(spm.get(4), 1))
        spec.type = type_text or type_code or None
        spec.collected_at = _coerce_datetime(unescape(component(spm.get(17), 0)))  # SPM-17
        canonical.specimen.append(spec)


def _parse_observations(msg, canonical: CanonicalMessage) -> None:
    for obx in msg.all("OBX"):
        obs = Observation()
        code = _test_code(obx.get(3))  # OBX-3
        if code is not None:
            obs.code = code
        value = unescape(component(obx.get(5), 0))  # OBX-5
        if value not in ("", None):
            obs.value = _coerce_value(value)
        obs.unit = unescape(component(obx.get(6), 0)) or None      # OBX-6
        obs.reference_range = unescape(obx.get(7)) or None         # OBX-7
        status = unescape(component(obx.get(11), 0))               # OBX-11
        obs.status = _OBX_STATUS.get(status, status or None)
        obs.observed_at = _coerce_datetime(unescape(component(obx.get(14), 0)))  # OBX-14

        abnormal = [unescape(c) for c in split_subcomponents(str(obx.get(8) or ""), "~")]  # OBX-8 abnormal flags
        abnormal = [a for a in abnormal if a]
        if abnormal:
            obs.extensions = {"abnormal_flags": abnormal}

        canonical.observations.append(obs)