# Field Mapping Reference

This document describes how wire formats map onto the engine's canonical model,
and how to write mapping rules that reference those fields by name.

The engine is a **pivot architecture**: every inbound format (HL7 v2, FHIR R4,
JSON, …) is decoded *into* one canonical model, the pipeline only ever works
with that model, and every outbound format is serialized *from* it.

```text
HL7 v2 ─┐                          ┌─ HL7 v2
FHIR R4 ─┤  decode ──> Canonical ──┼─ FHIR R4
JSON    ─┘              model      └─ JSON
              (the only thing the
              mapper sees)
```

> **Healthcare note.** Mapping rules only *move and reshape* existing data.
> The engine has no facility for inventing clinical values, and this document
> intentionally contains no examples of fabricating results. Never synthesize
> observations, identifiers, or coded values in an integration — an
> unknown/missing value must stay missing or be surfaced as an error, not
> replaced with a placeholder that could be mistaken for real data.

---

## 1. The mapping vocabulary: canonical paths

A `FieldMapper` rule targets the canonical message's JSON form. Field names in
rules are **dotted paths** into that document (the pydantic model attribute
names), exactly as returned by `CanonicalMessage.model_dump(mode="json")`.

```json
{ "source": "patient.name", "target": "order.ordering_provider.value" }
```

- Dict keys are separated by `.`
- List items are indexed by number: `observations.0.value`
- Enrichment reference data is addressed under `lookups.<lookup_name>.<field>`

There is intentionally **no `raw.*` namespace** — raw wire content is available
only for audit/replay, never as a transformation source.

### The `Identifier` building block

Identifiers are typed objects, not bare strings. Most clinically relevant
fields are `Identifier`s:

| Path suffix | Meaning | Required |
|---|---|---|
| `value` | the code/ID text (e.g. `12345`, `4544-3`) | ✅ |
| `system` | namespace / OID / URI that defines `value` | ❌ |
| `type` | role label: `MRN`, `SSN`, `PLACER`, `FILLER`, `PROVIDER`, `SPECIMEN` | ❌ |

So to touch a specific ID you always address it down to `.value`:
`order.accession.value` — never `order.accession` (which is an object).

## 2. Canonical model field reference

Top-level: `patient`, `encounter`, `order`, `specimen[]`, `observations[]`,
`metadata`, `extensions`.

### `patient`

| Path | Type | Meaning |
|---|---|---|
| `patient.identifiers[]` | `Identifier` list | MRN/SSN/… |
| `patient.name` | `str` | display name |
| `patient.dob` | `date` | date of birth |
| `patient.gender` | `str` | gender code |

### `encounter`

| Path | Type | Meaning |
|---|---|---|
| `encounter.identifiers[]` | `Identifier` list | |
| `encounter.visit_number` | `Identifier` | visit number |
| `encounter.started_at` | `datetime` | |

### `order`

| Path | Type | Meaning |
|---|---|---|
| `order.identifiers[]` | `Identifier` list | placer/filler ids |
| `order.accession` | `Identifier` | accession number (filler) |
| `order.requested_at` | `datetime` | |
| `order.priority` | `str` | `routine` / `stat` / `asap` / `preoperative` |
| `order.ordering_provider` | `Identifier` | ordering clinician |
| `order.items[]` | `OrderItem` list | ordered tests/services |

### `order.items[]` (`OrderItem`)

| Path | Type | Meaning |
|---|---|---|
| `order.items.0.identifiers[]` | `Identifier` list | |
| `order.items.0.code` | `Identifier` | ordered test code (LOINC) |
| `order.items.0.requested_at` | `datetime` | |
| `order.items.0.priority` | `str` | |

### `specimen[]`

| Path | Type | Meaning |
|---|---|---|
| `specimen.0.identifiers[]` | `Identifier` list | specimen ids |
| `specimen.0.type` | `str` | specimen type text |
| `specimen.0.collected_at` | `datetime` | collection time |

### `observations[]`

| Path | Type | Meaning |
|---|---|---|
| `observations.0.identifiers[]` | `Identifier` list | |
| `observations.0.code` | `Identifier` | LOINC (or system) code |
| `observations.0.status` | `str` | see §6 status table |
| `observations.0.value` | `str`/`int`/`float`/`bool`/`null` | result value |
| `observations.0.unit` | `str` | units |
| `observations.0.reference_range` | `str` | reference range text |
| `observations.0.observed_at` | `datetime` | time of result |
| `observations.0.extensions{}` | `dict` | vendor details, e.g. `abnormal_flags` |

### `metadata`

| Path | Type | Meaning |
|---|---|---|
| `metadata.format` | `str` | `hl7v2` / `fhir-r4` / … |
| `metadata.version` | `str` | e.g. `2.5.1` |
| `metadata.profile` | `str` | e.g. `ORU_R01` |
| `metadata.message_type` | `str` | e.g. `ORU^R01` |
| `metadata.source` | `str` | sending application |
| `metadata.message_id` | `str` | message control id / bundle id |
| `metadata.received_at` | `datetime` | transport receive time |

## 3. JSON codec — identity mapping

`JsonCodec` validates inbound JSON directly against the canonical model. The
JSON keys **are** the canonical field names — there is no per-field translation.

```json
{
  "patient": {
    "name": "Jane Doe",
    "dob": "1990-01-02",
    "gender": "F",
    "identifiers": [ { "value": "12345", "system": "HOSP", "type": "MRN" } ]
  },
  "order": {
    "accession": { "value": "FILLER-200", "type": "FILLER" },
    "priority": "routine"
  }
}
```

This is the format you see in `envelope.canonical_dict` and the format a mapper
reads/writes.

---

## 4. HL7 v2 (ORU_R01 / ADT_A01 / ORM_O01) mapping

Field numbers below use standard **1-based** HL7 numbering (the parser stores
segments as 0-based lists internally).

| Canonical field | HL7 source | Notes |
|---|---|---|
| `metadata.format` | *(fixed)* | `hl7v2` |
| `metadata.version` | MSH-12 | |
| `metadata.message_type` | MSH-9 | full `ORM^O01` / `ORU^R01` |
| `metadata.message_id` | MSH-10 | control id |
| `metadata.source` | MSH-3 | sending application |
| `patient.identifiers[]` | PID-3 | CX, repeatable via `~`; `value`=comp1, `system`=comp4, `type`=comp5 |
| `patient.name` | PID-5 | XPN `family^given` → `"given family"` when both present |
| `patient.dob` | PID-7 | date portion |
| `patient.gender` | PID-8 | |
| `encounter.visit_number` | PV1-19 | as `Identifier` |
| `order.identifiers[]` | ORC-2 / ORC-3 / OBR-2 / OBR-3 | placer → `type=PLACER`, filler → `type=FILLER` |
| `order.accession` | ORC-3 / OBR-3 | filler as `Identifier` |
| `order.requested_at` | OBR-7 | |
| `order.priority` | OBR-27 | code → label (see §6) |
| `order.ordering_provider` | OBR-16 | XCN, `type=PROVIDER` |
| `order.items[]` | OBR-4 | first OBR only (see §9) |
| `specimen[].identifiers[]` | SPM-2 | `type=SPECIMEN` |
| `specimen[].type` | SPM-4 | comp0 code or comp1 text |
| `specimen[].collected_at` | SPM-17 | |
| `observations[].code` | OBX-3 | CE → `Identifier` |
| `observations[].value` | OBX-5 | coerced to int/float/str |
| `observations[].unit` | OBX-6 | |
| `observations[].reference_range` | OBX-7 | |
| `observations[].extensions.abnormal_flags` | OBX-8 | list, split on `~` |
| `observations[].status` | OBX-11 | code → label (see §6) |
| `observations[].observed_at` | OBX-14 | |

## 5. FHIR R4 (Bundle) mapping

| Canonical field | FHIR location |
|---|---|
| `patient` | `Patient` entry |
| `order` | `ServiceRequest` entry |
| `specimen[]` | `Specimen` entry (one each) |
| `observations[]` | `Observation` entry (one each) |
| `metadata.message_id` | `Bundle.id` |

| Canonical field | FHIR field |
|---|---|
| `patient.identifiers[]` | `Patient.identifier[]` |
| `patient.name` | `Patient.name[0].text` |
| `patient.dob` | `Patient.birthDate` |
| `patient.gender` | `Patient.gender` |
| `order.identifiers[]` | `ServiceRequest.identifier[]` |
| `order.accession.value` | `ServiceRequest.accessionNumber` |
| `order.items[0].code` | `ServiceRequest.code.coding[0]` |
| `order.requested_at` | `ServiceRequest.occurrenceDateTime` |
| `order.ordering_provider` | `ServiceRequest.requester.identifier` |
| `specimen.identifiers[]` | `Specimen.identifier[]` |
| `specimen.type` | `Specimen.type.text` (fallback `type.coding[0].code`) |
| `specimen.collected_at` | `Specimen.collection.collectedDateTime` |
| `observations[].code` | `Observation.code.coding[0]` |
| `observations[].value` | `Observation.valueQuantity.value` **or** `valueString` |
| `observations[].unit` | `Observation.valueQuantity.unit` |
| `observations[].reference_range` | `Observation.referenceRange[0].text` |
| `observations[].observed_at` | `Observation.effectiveDateTime` |
| `observations[].extensions.abnormal_flags` | `Observation.interpretation[].coding[0].code` |

`Identifier` ↔ FHIR: `value` ↔ `value`/`code`, `system` ↔ `system`,
`type` ↔ `type.coding[0].code`.

---

## 6. Controlled vocabularies (code ↔ label)

### Priority (`order.priority`)

| HL7 (OBR-27) | canonical | FHIR (ServiceRequest.priority) |
|---|---|---|
| `R` | `routine` | `routine` |
| `S` | `stat` | `stat` |
| `A` | `asap` | `asap` |
| `P` | `preoperative` | `urgent` |

### Observation status (`observations[].status`)

| HL7 (OBX-11) | canonical | FHIR (Observation.status) |
|---|---|---|
| `F` | `final` | `final` |
| `P` | `preliminary` | `preliminary` |
| `C` | `corrected` | `corrected` |
| `R` | `registered` | `registered` |
| `A` | `amended` | `amended` |
| `X` | `cancelled_with_results` | `cancelled` |
| `U` | `unable_to_obtain` | `unknown` |
| `D` | `deleted` | `entered-in-error` |
| `S` | `revised` | `amended` |

## 7. Mapping rule syntax

Rules are stored as JSON in the `mappings` table (columns `rules`, pinned to a
`mapping_id` + `version`). Each rule:

```json
{
  "source":   "<canonical path | lookups.<name>.<field>>",
  "target":   "<canonical path>",
  "required":  false,
  "fn":        "Uppercase | Lowercase | Trim Whitespace | Format | Default",
  "fn_args":   { }
}
```

- `source` is read from the canonical form (or from enrichment `lookups`).
- `target` is written back into a copy of the canonical form; untouched fields
  survive, and the whole result is re-validated as a `CanonicalMessage`.
- `required: true` raises if the source is missing.
- `fn` applies a whitelisted transform; `fn_args` are its arguments.

Available functions (`nodes/transform/functions.py`):

| Function | Arguments | Effect |
|---|---|---|
| `Uppercase` | — | `str(value).upper()` |
| `Lowercase` | — | `str(value).lower()` |
| `Trim Whitespace` | — | `str(value).strip()` |
| `Format` | `fmt` (default `%Y%m%d%H%M%S`) | format a datetime/ISO string |
| `Default` | `default` | return `default` when value is `None` |

---

## 8. Example cases

### Example 1 — HL7 v2 ORU → FHIR R4 (pure codec conversion)

No mapping rules are required when the codecs share the canonical model. Point
the channel at the two codecs and the codec layer does the field mapping.

```json
{
  "channel_id": "oru_to_fhir",
  "name": "Lab results → FHIR",
  "inbound_transport": "mllp",
  "inbound_transport_config": { "host": "0.0.0.0", "port": 7777 },
  "inbound_codec": "hl7v2.5.1.ORU_R01",
  "outbound_codec": "fhir.r4",
  "destination": "http",
  "destination_config": { "endpoint_url": "https://.../fhir" },
  "retry_policy_id": "default"
}
```

An inbound `ORU^R01` with `OBX|…|4544-3^Glucose^LN|…|140.0|mg/dL…` becomes a
FHIR `Bundle` whose `Observation` carries:

```json
{ "code": { "coding": [ { "code": "4544-3", "system": "http://loinc.org" } ] },
  "valueQuantity": { "value": 140, "unit": "mg/dL" } }
```

(Verified by `tests/test_pipeline_formats.py`.)

### Example 2 — FHIR R4 → HL7 v2 ORU

The reverse direction uses the same canonical model; only the codec keys swap.

```json
{
  "channel_id": "fhir_to_oru",
  "name": "FHIR → LIS results",
  "inbound_transport": "http_webhook",
  "inbound_codec": "fhir.r4",
  "outbound_codec": "hl7v2.5.1.ORU_R01",
  "destination": "mllp",
  "destination_config": { "host": "lis.example", "port": 2575 },
  "retry_policy_id": "default"
}
```

### Example 3 — local test code → canonical LOINC via enrichment lookup

A reference table maps the sender's local test codes to their canonical codes.
The enrichment populates `lookups.ref`, then mapping rules copy the resolved
values onto the message.

Enrichment definition (stored via the `enrichments` table):

```json
{
  "enrichment_id": "local_to_loinc",
  "source_key_field": "order.items.0.code.value",
  "lookup_db_path": "/data/ref.db",
  "target_table": "test_map",
  "target_key_col": "local_code",
  "fields": ["loinc_code", "text"],
  "lookup_name": "ref"
}
```

Mapping rules (stored via the `mappings` table, referenced by the channel's
`mapping_id`/`mapping_version`):

```json
{
  "mapping_id": "map_local_codes",
  "rules": [
    { "source": "lookups.ref.loinc_code",
      "target": "order.items.0.code.value" },
    { "source": "lookups.ref.text",
      "target": "metadata.extensions.original_text" }
  ]
}
```

> Only real reference data read from an actual database is applied. If the
> lookup returns nothing, the target stays unchanged — nothing is invented.

### Example 4 — field-level transforms with functions

```json
{
  "mapping_id": "normalize",
  "rules": [
    { "source": "patient.name", "target": "patient.name", "fn": "Trim Whitespace" },
    { "source": "observations.0.unit", "target": "observations.0.unit", "fn": "Uppercase" },
    { "source": "metadata.received_at", "target": "metadata.extensions.received_hl7_ts",
      "fn": "Format", "fn_args": { "fmt": "%Y%m%d%H%M%S" } },
    { "source": "order.ordering_provider.value", "target": "order.ordering_provider.value",
      "fn": "Default", "fn_args": { "default": "UNKNOWN_PROVIDER" }, "required": true }
  ]
}
```

---

## 9. Known limitations

These are current-codec behaviors to be aware of when designing mappings; they
are not mapping-rule features.

1. **Single order item.** The HL7 v2 parser reads only the **first** `OBR`
   (`msg.first("OBR")`) and stores a single `order.items[0]` from OBR-4. Multiple
   ordered tests on one order are not fully round-tripped today.
2. **Profile header passthrough.** The HL7 serializer forces `MSH-9` for
   `ADT_A01` and `ORM_O01`, but for `ORU_R01` it uses `metadata.message_type`
   when present. A canonical message decoded from a non-ORU source and
   re-emitted with the `ORU_R01` codec keeps the source message type in MSH-9
   unless `metadata.message_type` is explicitly set to `ORU^R01`.
3. **`Identifier` objects in mappings.** Targets that are `Identifier`s must be
   assigned to their `.value`/`.system`/`.type` leaves; assigning a whole object
   to `order.accession` directly will fail final model validation.
4. **No list iteration in rules.** Each rule is one path. `FieldMapper` cannot
   "do X for every observation" — address each index explicitly (`.0`, `.1`, …)
   or use a dedicated node for collection-level transforms.
5. **Lossy JSON dates.** Canonical dates/datetimes are ISO strings in the JSON
   form used by mappers; use the `Format` function when a target needs a
   non-ISO text shape.