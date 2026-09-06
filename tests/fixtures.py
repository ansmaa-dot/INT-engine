"""Golden fixtures for the Pass 5 codec tests.

HL7 segments are assembled from explicit field lists so that field-number to
list-index mapping is exact (index N == field N; exception: MSH, where index 0
is the separator field). FHIR fixtures are plain R4 JSON bundles.
"""

_SEG = "\r"


def _fields(seg_name, *fields):
    """Build an HL7 segment line: segment name + pipe-delimited fields."""
    return "|".join([seg_name] + [str(f) for f in fields])


def oru_r01() -> str:
    """Golden ORU^R01 result message (v2.5.1) with patient, encounter, order,
    one specimen and two observations (numeric + textual)."""
    lines = [
        _fields(
            "MSH", "^~\\&", "LIS", "HOSP", "ORDER", "HOSP", "20240101103000", "",
            "ORU^R01", "CTRL001", "P", "2.5.1",
        ),
        # PID-3 carries two identifiers via a '~' repetition.
        _fields(
            "PID", "1", "", "12345^^^HOSP^MRN~67890^^^SOC^SSN", "",
            "Doe^Jane", "", "19900102", "F",
        ),
        # PV1-19 (visit number) is list index 19.
        "|".join(["PV1", "1", "O"] + [""] * 16 + ["VISIT999"]),
        _fields("ORC", "RE", "PLACER-100", "FILLER-200", "", "", "", "", "20240101103000"),
        # OBR-4 (idx4), OBR-7 (idx7), OBR-16 (idx16), OBR-27 (idx27).
        "|".join(
            ["OBR", "1", "PLACER-100", "FILLER-200", "CBC^Complete Blood Count^LN",
             "", "", "20240101103000"]
            + [""] * 8 + ["DR100^House^Robert"] + [""] * 10 + ["R"]
        ),
        # SPM-2 (idx2), SPM-4 (idx4), SPM-17 (idx17).
        "|".join(["SPM", "1", "SPEC-500", "", "BLD^Whole Blood^L"] + [""] * 12 + ["20240101090000"]),
        # OBX-3 (idx3), OBX-5 (idx5), OBX-8 abnormal flags (idx8), OBX-11 status (idx11),
        # OBX-14 observed time (idx14).
        _fields(
            "OBX", "1", "NM", "4544-3^Glucose^LN", "", "140.0", "mg/dL", "70-100",
            "H", "", "", "F", "", "", "20240101103000",
        ),
        _fields(
            "OBX", "2", "ST", "718-7^Hemoglobin^LN", "", "15.2", "g/dL", "12-16",
            "", "", "", "F", "", "", "20240101103000",
        ),
    ]
    return _SEG.join(lines)


def adt_a01() -> str:
    """Golden ADT^A01 admit message (v2.5.1)."""
    return _SEG.join([
        _fields(
            "MSH", "^~\\&", "REG", "HOSP", "ADT", "HOSP", "20240101110000", "",
            "ADT^A01", "ADT001", "P", "2.5.1",
        ),
        _fields(
            "EVN", "A01", "20240101110000",
        ),
        _fields(
            "PID", "1", "", "555000111^^^HOSP^MRN", "", "Smith^John", "", "19800707", "M",
        ),
        # PV1-2 patient class (idx2), PV1-19 visit number (idx19).
        "|".join(["PV1", "1", "I"] + [""] * 16 + ["VISIT123"]),
    ])


def fhir_bundle_dict(with_result: bool = True) -> dict:
    """A hand-authored FHIR R4 Bundle (Patient + ServiceRequest + Specimen +
    Observation) used as a golden inbound fixture and the target of the
    HL7 -> FHIR e2e path."""
    bundle = {
        "resourceType": "Bundle",
        "type": "collection",
        "id": "CTRL001",
        "entry": [
            {"resource": {
                "resourceType": "Patient",
                "identifier": [
                    {"system": "HOSP", "value": "12345", "type": {"coding": [{"code": "MRN"}]}},
                ],
                "name": [{"text": "Doe^Jane"}],
                "birthDate": "1990-01-02",
                "gender": "F",
            }},
            {"resource": {
                "resourceType": "ServiceRequest",
                "status": "active",
                "intent": "order",
                "identifier": [{"value": "PLACER-100", "type": {"coding": [{"code": "PLACER"}]}}],
                "accessionNumber": "FILLER-200",
                "code": {"coding": [{"system": "http://loinc.org", "code": "4544-3"}]},
                "occurrenceDateTime": "2024-01-01T10:30:00",
                "priority": "routine",
            }},
            {"resource": {
                "resourceType": "Specimen",
                "identifier": [{"value": "SPEC-500", "type": {"coding": [{"code": "SPECIMEN"}]}}],
                "type": {"text": "Whole Blood"},
                "collection": {"collectedDateTime": "2024-01-01T09:00:00"},
            }},
        ],
    }
    if with_result:
        bundle["entry"].append({"resource": {
            "resourceType": "Observation",
            "status": "final",
            "code": {"coding": [{"system": "http://loinc.org", "code": "4544-3"}]},
            "valueQuantity": {"value": 140, "unit": "mg/dL"},
            "referenceRange": [{"text": "70-100"}],
            "effectiveDateTime": "2024-01-01T10:30:00",
            "interpretation": [{"coding": [{"code": "H"}]}],
        }})
    return bundle


def fhir_bundle_json() -> str:
    import json

    return json.dumps(fhir_bundle_dict(), sort_keys=True)