# Codec-Aware Channel Authoring — UI Design Spec

- **Status:** Proposal (no code changes yet)
- **Scope:** Two user-experience improvements to the channel configuration UI:
  1. Inline authoring of mapping/enrichment definitions from inside the channel form.
  2. Codec-aware field pickers so users never hand-type canonical path names.
- **Related:** `FIELD_MAPPING.md` (field reference), `ARCHITECTURE.md`.

---

## 1. Background & problem

Today the channel form (`api/ui/templates/channel_form.html`) *selects* a
Mapping and an Enrichment by `mapping_id` + version. The actual definitions are
authored elsewhere:

- Mappings: a raw JSON `<textarea>` in the Definitions panel
  (`api/ui/definitions.py`) expecting `[{"source":"patient.name","target":…}]`.
- Enrichments: a raw form of DB fields (`source_key_field`, `lookup_db_path`,
  `target_table`, `target_key_col`, `fields[]`, `lookup_name`).

To write a mapping rule the user must already know canonical path names such as
`observations.0.value` or `order.ordering_provider.value`. That is the "empty
fields" pain point: the UI gives no hint what fields actually exist for the
selected inbound/outbound codec.

The design must preserve a deliberate property of the current data model:

> Mappings and enrichments are **reusable, versioned definitions**. A channel
> pins a specific version (`mapping_id` + `mapping_version`), so editing a
> definition creates a **new** version and never silently mutates other
> consumers.

---

## 2. Goals

- **G1** — authors can create or attach a mapping/enrichment without leaving the
  channel form (single-screen authoring).
- **G2** — authors pick source/target fields by friendly, codec-specific names
  instead of typing canonical paths.
- **G3** — authors can see the actual parsed fields of a real sample message and
  build rules by pointing at them.
- **G4** — enrichment columns and key columns are picked from lists, not typed
  as free text.
- **G5** — preserve the reusable/versioned definition model and its fail-fast
  validation. No behavioral contract on the pipeline changes.

---

## 3. Non-goals

- No change to the rules data shape (`[{source,target,fn,fn_args,required}]`) or
  to the enrichment schema. These are UI authoring improvements only.
- No coupling of inbound → outbound codecs (they stay independent).
- No fabricating/synthesizing clinical values anywhere in the authoring flow.

## 4. Feature 1 — Inline mapping / enrichment authoring

### 4.1 Channel form changes

The existing `Mapping` and `Enrichment` `<select>` controls gain an extra entry:

```text
Mapping:     [ None | <existing definitions…> | ── Create new… ── ]
Enrichment:  [ None | <existing definitions…> | ── Create new… ── ]
```

Choosing `Create new…` reveals an inline editor (the same builder described in
§5 for mappings; the enrichment form described in §5.4 for enrichments). On
save, the channel is atomically saved with the newly created definition pinned.

### 4.2 Backend helper

Add a helper that creates a definition and re-points the channel in one unit of
work. It reuses existing methods that already return the new version:

```python
version = registry.save_mapping(mapping_id, rules, description=…)
# then save the channel with mapping_id + mapping_version = version
```

- Suggested inline id convention: `<channel_id>__map` (and `<channel_id>__enrich`)
  to avoid surprise collisions and make the relationship obvious.
- On conflict (id already pinned elsewhere), either reuse only if identical, or
  auto-suffix (`__map2`) — decided during implementation. Reuse must never
  silently repoint an existing consumer.

### 4.3 What stays the same

- The Definitions panel remains for standalone reuse and version list/editing.
- The `mappings` / `enrichments` tables and their version-pinning semantics are
  unchanged. Only the *authoring entry point* moves closer to the channel.

## 5. Feature 2 — Codec-aware field selection

Three complementary layers, cheapest first.

### 5.1 Static field catalog per codec

Add a read-only description of the fields a codec's `parse()` produces (source
side) and/or `serialize()` consumes (target side). This turns the
`FIELD_MAPPING.md` tables into data.

Proposed API on the codec (or a sibling schema module to avoid touching the
transport-facing `Codec` contract):

```python
def describe_fields(self) -> list[FieldSpec]:
    """Return friendly, codec-specific field entries pointing at canonical paths."""
```

New endpoint:

```text
GET /ui/fields/<codec_key>
```

Response shape:

```json
[
  { "label": "MSH-10 — message id",      "path": "metadata.message_id",       "kind": "str" },
  { "label": "PID-3 — patient id",       "path": "patient.identifiers",        "kind": "identifier[]" },
  { "label": "PID-5 — patient name",     "path": "patient.name",              "kind": "str" },
  { "label": "OBR-4 — order test code",  "path": "order.items.0.code",        "kind": "identifier" },
  { "label": "OBX-3 — observation code", "path": "observations.0.code",       "kind": "identifier" },
  { "label": "OBX-5 — value",            "path": "observations.0.value",      "kind": "scalar" }
]
```

- Source picker uses the **inbound** codec's catalog.
- Target picker uses the **outbound** codec's catalog (still canonical paths,
  because the mapper writes canonical and the codec re-serializes; labels shown
  as "OBX-5 — value" for HL7, "Observation.valueQuantity" for FHIR, etc.).

### 5.2 Sample-based preview (parsed field tree)

Let the user paste a real sample (`MSH|…` block, JSON bundle, etc.) or pick a
recent envelope for the channel. The system runs the selected codec's `parse()`
and renders the resulting canonical document as a clickable tree.

```text
POST /ui/preview
{ "codec": "hl7v2.5.1.ORU_R01", "sample": "MSH|…" }
```

Response:

```json
{
  "ok": true,
  "tree": {
    "metadata": { "message_type": "ORU^R01", "message_id": "CTRL001" },
    "patient": { "name": "Jane Doe", "identifiers": [ {"value": "12345"} ] },
    "observations": [ { "value": 140, "unit": "mg/dL" } ]
  }
}
```

- This is nearly free: `codec.parse(sample).model_dump(mode="json")` (see
  `JsonCodec.serialize` for the exact dump form).
- It is the robust approach for `hl7v2.5.1.UNDEFINED` ("any"), where a static
  catalog cannot enumerate fields — the sample shows exactly what is present.
- It surfaces the correct list indices (`.0`, `.1`) that users otherwise get wrong.

### 5.3 Visual "source → target" rule builder

Replace the raw-JSON rules textarea with two panels (source tree on the left,
target catalog/tree on the right). Each mapped field produces one rule:

```text
[ patient.name ]  ──(Uppercase)──>  [ OBX-5 / observations.0.value ]
```

- Clicking a source and a target appends `{source, target}`.
- An optional per-rule `fn` select applies `Uppercase` / `Lowercase` /
  `Trim Whitespace` / `Format` / `Default`.
- A `required` toggle maps to the `required` flag.
- Keep an **"Advanced (JSON)"** toggle that exposes the existing textarea verbatim
  for power users; both editors feed the same `rules` list.

No backend change: the builder serializes to the identical `rules` JSON that
`FieldMapper.transform()` already consumes.

### 5.4 Enrichment column discovery

Add a read-only "list columns" helper above the existing
`db_adapter` (`nodes/enrichment/db_adapter.py` already validates identifiers):

```text
POST /ui/enrichments/columns
{ "db_type": "sqlite", "connection_string": null,
  "lookup_db_path": "/data/ref.db", "target_table": "test_map" }
```

Response: the column names for the table, so `target_key_col` becomes a
`<select>` and `fields[]` becomes a checkbox/multi-select list, instead of typed
strings. SQLite → `PRAGMA table_info(...)`; PostgreSQL/MySQL →
`information_schema.columns`.

---

## 6. Endpoint summary

| Endpoint | Method | Purpose |
|---|---|---|
| `/ui/fields/<codec>` | GET | static field catalog for a codec |
| `/ui/preview` | POST | parse a sample into a canonical tree |
| `/ui/enrichments/columns` | POST | list columns for a table |
| *(no new)* | POST | `save_mapping` / `save_enrichment` already exist; add inline wrapper |

All are read-only helpers except the existing save paths; they live alongside
the current Blueprints (`api/ui/channels.py`, `api/ui/definitions.py`).

## 7. Data model & invariants (preserved)

- `mappings.rules` remains a JSON array of `{source,target,fn,fn_args,required}`.
- `enrichments` schema is unchanged.
- Channel rows still store `mapping_id`/`mapping_version` and
  `enrichment_id`/`enrichment_version`; the inline flow simply populates both on
  the same save.
- `validate_channel_definition()` continues to reject unknown codecs, missing
  definition versions, unknown transports/destinations, and missing retry policy.

---

## 8. Security & rendering notes

- The channel form must keep the existing single-`render_template()` rule
  (see the warning in `_render_channel_form`). The tree/builder render through
  the same Jinja path — never `render_template_string` over user text.
- HTML-escape all sample/originating values (`_esc` from `api/ui/helpers.py`).
- Column names and tables remain validated by `is_valid_identifier` before any
  query interpolation; the new column-listing endpoint must apply the same guard.
- Sample parsing/preview must run through the same codec/`DecodeError` paths as
  production so a bad sample fails the same way (typed errors, no stack traces
  leaked to the UI).

---

## 9. Gotchas

1. **Registry key casing** — `codec_label()` lowercases keys
   (`hl7v2.5.1.oru_r01`); the field catalog and `/ui/fields` route must match the
   actual registry keys (`hl7v2.5.1.ORU_R01`). Normalize in one place.
2. **`Identifier` leaves** — paths pointing at identifiers must expose `.value` /
   `.system` / `.type` leaves; assigning a whole object to `order.accession`
   fails model validation. The builder should drop the leaf granularity onto the
   target automatically.
3. **`UNDEFINED` codec** — has no fixed catalog; fall back to sample preview or
   a generic "any HL7 v2" catalog.
4. **List indexing** — sample preview discovers real indices; static catalog
   entries must not imply an index exists (use `observations.0.code` only as an
   example and let the preview author the concrete path).

---

## 10. Phasing

1. **P1 — static catalog.** Codec `describe_fields()` + `GET /ui/fields/<codec>`;
   wire a `<datalist>`/dropdown into the source & target inputs. Kills the
   "must know path names" problem immediately.
2. **P2 — sample preview + rule builder.** `POST /ui/preview`, clickable trees,
   and the two-panel builder (still emitting the same `rules` JSON).
3. **P3 — inline authoring.** "Create new…" mapping/enrichment in the channel
   form via the `save_*` wrapper.
4. **P4 — enrichment discovery.** `POST /ui/enrichments/columns` and dropdown
   selectors for key column/fields.

---

## 11. Acceptance criteria

- A user can create a channel and, without leaving the form, attach a mapping
  that was authored by clicking fields from an HL7 sample on source and an FHIR
  (or HL7) catalog on target.
- The produced `rules` JSON is byte-compatible with what `FieldMapper` expects;
  existing `tests/test_runner.py` mapping tests pass unchanged.
- Inline-created definitions appear in the Definitions panel with a new version
  and do not alter other channels' pinned versions.
- A bad sample in preview returns a typed decode error, not an internal traceback
  or an empty tree.
- Enrichment key column and fields are selectable from a discovered column list,
  and an invalid table/column still fails `is_valid_identifier` validation.