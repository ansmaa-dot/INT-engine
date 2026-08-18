"""CanonicalMessage → HL7 v2 serialization for the laboratory profiles
(ORU_R01, and ADT_A01 basics), v2.5.1. All escaping/segment building lives
here — inside the codec, never the destination/transport.
"""
from __future__ import annotations

from datetime import date, datetime

from core.model import CanonicalMessage, Encounter, Order, PatientSummary, Specimen
from nodes.codec.hl7v2.er7 import escape

_LOINC_SYSTEM = "http://loinc.org"
_OBX_STATUS_INV = {
    "corrected": "C", "final": "F", "preliminary": "P", "registered": "R",
    "amended": "A", "cancelled_with_results": "X", "unable_to_obtain": "U",
    "deleted": "D", "revised": "S",
}
_PRIORITY_INV = {"routine": "R", "stat": "S", "asap": "A", "preoperative": "P"}


def _dtstr(value) -> str:
    if isinstance(value, datetime):
        return value.strftime("%Y%m%d%H%M%S")
    if isinstance(value, date):
        return value.strftime("%Y%m%d")
    return ""


def _f(*parts) -> str:
    """Join components with '^', escaping each part."""
    return "^".join(escape(p) if p is not None else "" for p in parts)


def serialize_message(canonical: CanonicalMessage, profile: str = "ORU_R01") -> str:
    lines = [_msh(canonical, profile)]
    if canonical.patient is not None:
        lines.append(_pid(canonical.patient))
    if canonical.encounter is not None and canonical.encounter.visit_number:
        lines.append(_pv1(canonical.encounter))
    if profile == "ADT_A01":
        lines.append("EVN|A01||")
    if canonical.order is not None:
        lines.append(_obr(canonical.order))
    for spec in canonical.specimen:
        lines.append(_spm(spec))
    for obs in canonical.observations:
        lines.append(_obx(obs))
    return "\r".join(lines)


def _msh(canonical: CanonicalMessage, profile: str) -> str:
    meta = canonical.metadata
    message_type = (meta.message_type or profile.replace("_", "^"))
    if profile == "ADT_A01":
        message_type = "ADT^A01"
    control_id = meta.message_id or "INTENGINE-1"
    version = meta.version or "2.5.1"
    # indices: 0=MSH 1=enc 2=sendapp 3..5 empty 6=MSH-7 dt 7=empty
    #          8=MSH-9 msgtype 9=MSH-10 control 10=MSH-11 P 11=MSH-12 version
    parts = ["MSH", "^~\\&", "INTENGINE", "", "", "", _dtstr(datetime.now()),
             "", message_type, escape(control_id), "P", escape(version)]
    return "|".join(parts)


def _coding_system(identifier) -> str:
    system = (identifier.system if identifier else None) or ""
    return "LN" if system == _LOINC_SYSTEM else escape(system)


def _identifier_cx(identifier) -> str:
    if identifier is None:
        return ""
    return _f(identifier.value, "", "", identifier.system or "", identifier.type or "")


def _pid(patient: PatientSummary) -> str:
    ids = "~".join(_identifier_cx(i) for i in patient.identifiers)
    name = escape(patient.name or "")
    dob = _dtstr(patient.dob)
    gender = escape(patient.gender or "")
    return f"PID|1|{ids}|{ids}||{name}||{dob}|{gender}"


def _pv1(encounter: Encounter) -> str:
    fields = ["PV1", "1", "O"] + [""] * 17  # 20 fields (indices 0..19); PV1-19 is index 19
    if encounter.visit_number is not None:
        fields[19] = escape(encounter.visit_number.value)
    return "|".join(fields)
def _obr(order: Order) -> str:
    fields = ["OBR"] + [""] * 27  # indices 0..27 (OBR-28)
    fields[1] = "1"  # OBR-1 set id
    placer = next((i for i in order.identifiers if i.type == "PLACER"), None)
    filler = order.accession or next((i for i in order.identifiers if i.type == "FILLER"), None)
    if placer is not None:
        fields[2] = _f(placer.value, placer.system or "")       # OBR-2
    if filler is not None:
        fields[3] = _f(filler.value, filler.system or "")       # OBR-3
    if order.items:
        code = order.items[0].code
        if code is not None:
            fields[4] = _f(code.value, "", _coding_system(code))  # OBR-4 CE
    fields[7] = _dtstr(order.requested_at)                      # OBR-7
    if order.ordering_provider is not None:
        fields[16] = escape(order.ordering_provider.value)      # OBR-16
    fields[27] = escape(_PRIORITY_INV.get(order.priority, order.priority or ""))  # OBR-27
    return "|".join(fields)


def _spm(specimen: Specimen) -> str:
    fields = [""] * 18  # SPM-17 is index 17
    fields[0] = "SPM"
    fields[1] = "1"  # SPM-1 set id
    specimen_id = next((i.value for i in specimen.identifiers if i.type == "SPECIMEN"), None) \
        or (specimen.identifiers[0].value if specimen.identifiers else None)
    if specimen_id:
        fields[2] = escape(specimen_id)                          # SPM-2
    fields[4] = escape(specimen.type or "")                      # SPM-4 type
    fields[17] = _dtstr(specimen.collected_at)                   # SPM-17 collected
    return "|".join(fields)


def _obx(observation) -> str:
    # OBX field N -> index N (index 0 = "OBX")
    fields = [""] * 15  # OBX-14 is index 14
    value_type = "NM" if isinstance(observation.value, (int, float)) else "ST"
    fields[0] = "OBX"
    fields[1] = "1"                              # OBX-1 set id
    fields[2] = escape(value_type)               # OBX-2 value type
    if observation.code is not None:
        fields[3] = _f(observation.code.value, "", _coding_system(observation.code))  # OBX-3
    fields[5] = escape(observation.value) if observation.value is not None else ""     # OBX-5
    fields[6] = escape(observation.unit or "")                   # OBX-6 unit
    fields[7] = escape(observation.reference_range or "")        # OBX-7 reference range
    abnormal = observation.extensions.get("abnormal_flags") or []
    if abnormal:
        fields[8] = "~".join(escape(a) for a in abnormal)        # OBX-8 abnormal flags
    fields[11] = escape(_OBX_STATUS_INV.get(observation.status, observation.status or "F"))  # OBX-11
    fields[14] = _dtstr(observation.observed_at)                 # OBX-14 observed time
    return "|".join(fields)

