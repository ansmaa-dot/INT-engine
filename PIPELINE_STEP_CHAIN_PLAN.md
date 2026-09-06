# Pipeline Step-Chain — Architectural Implementation Plan

> **Status:** P0 ✅, P1 ✅, P2 ✅ & P3 ✅ — proceeding to P4 (UI backend).
> **Scope:** Replace the fixed `enrich → transform` slots with an ordered, typed
> `pipeline` list per channel, interpreted by `ChannelRunner` at runtime, with a
> matching step-chain builder in the channel UI.
>
> **Backward compatibility:** none. We reset the database and break existing
> schemas/configs; there is no migration path and no support for legacy channel
> definitions.

---

## 1. Overview & scope

The channel `ChannelRunner.process_one()` in `engine/runner.py` hardcodes a
single optional enrichment + a single optional mapping:
`decode → validate → enrich → transform → validate → encode → deliver`.

This feature replaces those two fixed slots with an **ordered list of typed
steps** (`"pipeline": [...]`) that `ChannelRunner` interprets instead, with four
step types:

| Step type | Behavior | I/O |
|---|---|---|
| `enrich` | existing `BatchLookup`, now **repeatable** (multiple DB lookups) | read-only |
| `transform` | existing `FieldMapper`, now **repeatable** | none (pure) |
| `filter` | new: sandboxed boolean expression; `false` → route per `on_fail` | none (pure) |
| `assert` | same expression engine; post-transform validation; `on_fail: retry \| dead_letter` | none (pure) |

Each step supports two authoring modes:

- **inline (default)** — config lives directly in the channel's `pipeline` list;
  auto-versioned under the hood; no user-facing `mapping_id`/`enrichment_id`;
  editable in place.
- **shared** — user "promotes" an inline step to a named, versioned definition so
  multiple channels can reference the same pinned version.

Invariants preserved:

- **Fail-fast validation** at save time (unknown ref / unknown expression field →
  rejected before runtime).
- **`PipelineError` stage+code classification per step**, with `step_id` /
  `step_type` added to error context.
- **Full versioning/audit** of every step.
- **Enrich is the only step with I/O** (read-only), so the whole chain is
  deterministic and safely **replayable for dry-run/testing**.

The UI is redesigned to match `channel_buildup_mockup.html` (note: the actual
filename in the repo root is `channel_buildup_mockup.html`, not
`channel_builder_mockup.html`).

**No new third-party dependency.** The expression evaluator is a small,
self-contained JSONLogic-style parser (pure Python, no `eval`/`exec`), consistent
with the "no dynamic scripting" rule already enforced by
`nodes/transform/functions.py`.

---

## 2. Current state (review findings)

- **`engine/runner.py`** — `process_one()` hardcodes the sequence
  (`runner.py:49-93`); `mapper`/`enricher` are single optional constructor args;
  retry-vs-DLQ is decided by the global `PERMANENT_STAGES` set (`runner.py:23`).
- **`engine/config_loader.py`** — `channels` table holds
  `mapping_id/mapping_version` + `enrichment_id/enrichment_version`; reusable
  definitions live in separate `mappings` / `enrichments` tables pinned by
  version; `_row_to_config` resolves refs; `_build_enrichment` does build-time DB
  validation; `build_runner` assembles `FieldMapper` + `BatchLookup`.
- **`core/errors.py`** — clean typed taxonomy (`stage`/`code`/`context`/`cause`),
  but no step identity and no retryability flag.
- **`core/canonical_paths.py`** — `resolve_path`/`assign_path` are the dotted-path
  contract; `FIELD_MAPPING.md` documents the full canonical field reference.
- **`docs/ui_field_picker.md`** — already spec'd the codec-aware field picker
  (status: proposal, no code). Its gotchas still apply (codec-key casing,
  `Identifier` leaf granularity, list-index care).
- **`channel_buildup_mockup.html`** — demonstrates the step-chain builder UI
  (collapsible `step-card`s, `+ Add Step` menu with 4 types, mapping table,
  filter/assert condition rows, "Available Fields" chips with human labels). Its
  inline CSS should be ported to `api/static/engine.css`.
- **No JSONLogic/expression library installed** — `requirements.txt` is
  flask/requests/sqlalchemy/paramiko/pydantic/pytest only.

---

## 3. Core design decisions (locked — D1 through D10)

### D1 — Hard reset, no legacy path

There is **no legacy-schema detection, no fallback for old channel rows, no
branching on old shapes**. The database carries a single schema stamp
(SQLite `PRAGMA user_version`, or a minimal `_meta` table). On **any** mismatch
with the target schema version, the affected tables are unconditionally dropped
and recreated, then re-seeded:

```text
DROP TABLE IF EXISTS channels;
DROP TABLE IF EXISTS pipeline_steps;
DROP TABLE IF EXISTS shared_steps;
-- recreate + seed_defaults()
```

No column inspection, no per-row migration, no `ALTER TABLE` fallback. The old
`mappings` / `enrichments` tables are superseded by `shared_steps` (D3) and are
dropped along with any legacy `channels` shape.

### D2 — Stable step identity (`step_id`), position decoupled from identity

Steps are keyed by a **stable `step_id` (UUID, assigned once when the step is
created, carried across reorders and edits)** — **not** by `step_index`.

Position (ordering) is stored separately, in the `channels.pipeline` JSON array
itself. Every element of that array carries its `step_id`, so reordering simply
permutes the array: nothing is re-keyed, no version bumps, identity is preserved.

**Why:** an index-based key `(channel_id, step_index, version)` breaks the moment
a step is inserted or reordered, because every subsequent step's index shifts —
forcing every historical row to be re-keyed and silently detaching a step from its
own version history. A UUID is position-independent.

### D3 — One generic `shared_steps` table (no four parallel tables)

Shared, named, versioned definitions for **all four kinds** live in a single
table keyed by `(kind, id, version)` instead of four near-identical tables:

```text
kind ∈ { mapping, enrichment, filter, assert }
```

This replaces the existing `mappings` / `enrichments` tables (kind `mapping` /
`enrichment`) **and** the two new `filters` / `asserts` tables from the earlier
draft. `retry_policies` is **not** folded in — it is a channel-level policy, not a
pipeline step.

Kind ↔ runtime step type mapping:

| runtime `type` | `shared_steps.kind` |
|---|---|
| `enrich` | `enrichment` |
| `transform` | `mapping` |
| `filter` | `filter` |
| `assert` | `assert` |

**`shared_steps` rows are immutable once written.** Editing a shared definition
always inserts a new `(kind, id, version)` row; an existing row is never
`UPDATE`d in place. This is required for D5.3's audit/replay guarantee — if a
pinned version could be edited after the fact, every `pipeline_steps` snapshot
and every historical replay that resolved against it would silently drift from
what actually ran. `_next_version()` collisions under concurrent edits to the
same `(kind, id)` should surface as a clean "this definition was edited by
someone else — reload" error, not an unhandled `IntegrityError`.

### D4 — `channels.pipeline` is the ordering source of truth

The channel row stores an **ordered JSON array** of steps. `pipeline_steps` is
append-only history keyed by `(step_id, version)`; it never encodes order.
Ordering authority lives solely in the `channels.pipeline` array index.

### D5 — Retry/permanence driven per step, not by a global stage set

Errors gain an explicit `retryable` flag; each step's `on_fail` sets it. The
global `PERMANENT_STAGES` set remains only as the fallback for the fixed
decode/validate/encode/deliver phases.

### D6 — `discard` is a terminal, observable state

Filter `on_fail=discard` routes to a new `MessageState.DISCARDED` + a `discarded`
audit event (never a silent drop, always replayable). Locked per D9 below — a
filtered-out message and a failed message are different things and must not
share a state or a metric.

### D7 — Dry-run via a pure core

`process_one()` is split into a **pure** `process_envelope(envelope, *,
deliver=True, destination=None)` and a thin queue/mutation wrapper
`process_one()`. A `run_dry(envelope)` uses the pure core with `deliver=False`
and no queue writes. Enrich is already read-only (`BatchLookup`), so the chain is
replayable.

### D8 — `semantics` is retired

The free-text `semantics` JSON on the channel is superseded by explicit `assert`
steps and is removed from the schema/UI.

### D9 — `discard` representation (locked)

**`MessageState.DISCARDED` + a `discarded` audit event.** Not DLQ-with-reason.

A message that was correctly excluded by a filter (by design) and a message that
failed and needs attention are different things. Routing both through
`dead_letter` would pollute the DLQ count — which `channel_health` and the
dashboard already use as an at-a-glance health signal — with expected,
non-error exclusions, making that metric noisy exactly where its value is
being able to trust it. The cost is one enum value and one audit event type;
the payoff is a DLQ count that always means "something needs attention."

### D10 — `on_fail` vocabulary (locked)

```text
filter.on_fail ∈ { dead_letter, discard }
assert.on_fail ∈ { retry, dead_letter }
```

**Note on UI labeling vs. backend value:** the mockup's filter copy reads "Route
to review queue," while the stored value is `dead_letter` — these are **the same
terminal action**, just worded differently per step type because the framing
reads more naturally in context (a filter "sends it for review"; an assert
"failed validation, dead-letter it"). Both land in the existing DLQ
table/UI (`api/ui/dlq.py`) — there is **no separate "review queue" mechanism**.
P5 (`channel_form.html`) must map the friendlier filter-step label to the same
`dead_letter` enum value the assert step uses; the `<select>` options differ in
display text only, never introduce a third stored state to match the copy.

---

## 4. Target data model (SQLite)

```sql
-- Orchestration-only channel row. `pipeline` is the ordered step array
-- and therefore the authority for step position (D4).
CREATE TABLE channels (
  channel_id               TEXT PRIMARY KEY,
  name                     TEXT NOT NULL,
  enabled                  INTEGER DEFAULT 1,
  status                   TEXT DEFAULT 'running',
  concurrency              INTEGER DEFAULT 1,
  inbound_transport        TEXT NOT NULL,
  inbound_transport_config TEXT,
  inbound_codec             TEXT NOT NULL,
  outbound_codec            TEXT NOT NULL,
  destination               TEXT NOT NULL,
  destination_config        TEXT,
  retry_policy_id           TEXT,
  pipeline                  TEXT NOT NULL DEFAULT '[]'   -- ordered steps
);

-- Single generic shared-definition table (D3): replaces mappings/enrichments
-- and avoids filters/asserts duplication. Rows are append-only / immutable
-- once written (D3).
CREATE TABLE shared_steps (
  kind        TEXT NOT NULL,     -- mapping | enrichment | filter | assert
  id          TEXT NOT NULL,
  version     INTEGER NOT NULL,
  config      TEXT NOT NULL,     -- JSON, shape depends on kind (see §5.2)
  description TEXT,
  created_at  TEXT NOT NULL,
  updated_at  TEXT NOT NULL,
  PRIMARY KEY (kind, id, version)
);

-- Append-only step history, keyed by stable step_id (D2). Never encodes order.
CREATE TABLE pipeline_steps (
  step_id     TEXT NOT NULL,     -- UUID, assigned once, survives reorder/edit
  channel_id  TEXT NOT NULL,
  version     INTEGER NOT NULL,
  type        TEXT NOT NULL,     -- enrich | transform | filter | assert
  config      TEXT NOT NULL,     -- resolved definition snapshot (JSON)
  provenance  TEXT,              -- {"kind":..,"id":..,"version":..} or NULL
  description TEXT,
  created_at  TEXT NOT NULL,
  updated_at  TEXT NOT NULL,
  PRIMARY KEY (step_id, version)
);
CREATE INDEX idx_pipeline_steps_channel ON pipeline_steps (channel_id);

-- unchanged
-- retry_policies(retry_policy_id PK, max_retries, base_backoff_seconds, description)
-- queue / audit_log / idempotency_keys / heartbeat (core/queue.py) unchanged
-- MessageState gains DISCARDED (D6/D9); queue/audit_log record a `discarded` event
```

**Reset contract (D1).** SQLite `PRAGMA user_version` (or a one-row `_meta`
table) is the single schema stamp. On mismatch: unconditionally `DROP TABLE IF
EXISTS channels, pipeline_steps, shared_steps`, recreate, re-seed. No column/
row shape inspection, no migration, no legacy branch.

---

## 5. Step authoring model

### 5.1 `channels.pipeline` element shapes

```jsonc
// inline (default)
{ "step_id": "11111111-1111-1111-1111-111111111111",
  "type": "enrich",
  "config": {
    "source_key_field": "patient.identifiers.0.value",
    "lookup_db_path": "/data/ref.db",
    "target_table": "patients",
    "target_key_col": "mrn",
    "fields": ["full_name", "provider"],
    "lookup_name": "pt",
    "db_type": "sqlite"
  } }

// shared (pinned version in the generic shared_steps table)
{ "step_id": "22222222-2222-2222-2222-222222222222",
  "type": "transform",
  "shared": { "kind": "mapping", "id": "map_local_codes", "version": 3 } }
```

`type` is the runtime dispatch key (`enrich|transform|filter|assert`). The
presence of `config` vs `shared` distinguishes inline vs shared. On save,
shared steps are validated to exist at the pinned `(kind, id, version)`.

### 5.2 `shared_steps.config` shapes by kind

```text
mapping    → { "rules": [ {source, target, fn?, fn_args?, required?} ] }
enrichment → { source_key_field, lookup_db_path, target_table, target_key_col,
               fields[], lookup_name, db_type?, connection_string? }
filter     → { "expression": <JSONLogic>, "on_fail": "dead_letter" | "discard" }
assert     → { "expression": <JSONLogic>, "on_fail": "retry" | "dead_letter" }
```

### 5.3 Inline auto-versioning and audit

- On each `save_channel_definition`, every step (inline **and** shared) writes a
  new `pipeline_steps` row under the same `step_id` with
  `version = (max(version) for that step_id) + 1`.
- `config` stores the **resolved** definition snapshot (for shared steps, the
  `shared_steps` config resolved at save time), so replay/audit never depends on
  a later change to a shared definition. This is guaranteed by D3's immutability
  rule — a pinned `shared_steps` version can never change out from under a past
  snapshot.
- `provenance` records the shared ref for traceability (`NULL` for inline).
- `description` is optional human text shown in the step card header.

### 5.4 Promote-to-shared flow

A step card's `⋮` menu offers **"Promote to shared definition"**. This creates a
`shared_steps` row (kind per §3 D3 mapping, `id` chosen by the user, next
version) from the step's current resolved config, then rewrites the channel's
pipeline element to `{step_id, type, shared:{kind,id,version}}`. The step_id is
retained (identity survives promotion); a new `pipeline_steps` version is written.

---

## 6. Expression engine (`core/expression.py`)

Pure-Python, JSONLogic-style, zero `eval`/`exec`.

- **Operator whitelist (closed):** comparisons `== != > >= < <= in contains`;
  logic `and or not if`; existence `exists missing empty not_empty truthy`;
  value access `var` (dotted canonical path via `core/canonical_paths.
  resolve_path`) and `literal`. **No function-registration API** — new operators
  are code changes only.
- `evaluate(expr, data: dict) -> bool` — recursive; raises a typed error on any
  unknown operator.
- `validate_expression(expr, field_catalog) -> list[str]` — static walk: operator
  whitelisted? every `var` path resolves to a known canonical field? This is what
  produces fail-fast "unknown expression field → rejected".

```jsonc
// example: assert result value present, else retry
{ "expression": { "not_empty": [ {"var": "observations.0.value"} ] },
  "on_fail": "retry" }
```

The filter/assert UI presents friendly `Field / Condition / Value` dropdowns and
serializes them to this JSONLogic form server-side.

---

## 7. Field catalog & picker (`core/field_catalog.py`)

- Single static source of truth mirroring `FIELD_MAPPING.md`:
  `FieldDescriptor(path, label, kind, group)` where `kind ∈ {str, datetime,
  identifier_value, identifier_system, identifier_type, list}`.
- Covers `patient`, `encounter`, `order` (+ `order.items[]`), `specimen[]`,
  `observations[]`, `metadata`, `extensions`, with human labels
  ("Patient name" → `patient.name`, "Accession Number" →
  `order.accession.value`, …).
- `catalog_for_codec(codec_key)` returns the canonical catalog plus optional
  per-codec source hints (HL7 `PID-5`, FHIR `Patient.name.text`, …). Codec keys
  must match `nodes.codec.registry` exactly (`hl7v2.5.1.ORU_R01`, `fhir.r4`, …);
  add `fhir.r4` to `helpers.codec_label`.
- `validate_canonical_path(path)`, `human_label(path)`, `canonical_path(label)`.
- Respects `docs/ui_field_picker.md` gotchas: never assign whole objects to
  `Identifier` leaves (always address `.value`); the static catalog must not imply
  a list index exists.

---

## 8. Prioritized implementation plan

### P0 ✅ — Error taxonomy (`core/errors.py`)

Smallest, everything-depends-on-it change.

- Extend `PipelineError.__init__` with keyword-only `step_id: str | None = None`,
  `step_type: str | None = None`, `retryable: bool | None = None`; store as
  attributes and merge `step_id` / `step_type` into `self.context`.
- Add two subclasses:
  - `FilterError(PipelineError)` — `stage = "filter"`.
  - `AssertError(PipelineError)` — `stage = "assert"`.
- Add `MessageState.DISCARDED = "DISCARDED"` in `core/message.py` (D6/D9).

> `step_index` is intentionally **not** part of the error contract — the stable
> identity is `step_id` (D2). A `step_index` ordinal can be derived from
> `channels.pipeline` for UI display if needed.

**Acceptance:** `str(exc)` unchanged; existing tests pass; `exc.context` carries
`step_id`/`step_type`; `retryable` defaults to `None` ("use stage classification").

---

### P1 ✅ — Expression engine + field catalog (new modules)

Two new, dependency-free modules (pure functions → easy unit tests).

**`core/expression.py`** (§6): `OPERATORS`, `evaluate()`, `validate_expression()`.

**`core/field_catalog.py`** (§7): `FieldDescriptor`, `FIELD_CATALOG`,
`catalog_for_codec()`, `validate_canonical_path()`, `human_label()`,
`canonical_path()`.

**Acceptance:** `eval`/`exec` never appear; bad op / unknown field return typed
validation errors; `tests/test_expression.py`, `tests/test_field_catalog.py` green.

### P2 ✅ — Database schema + config layer (`engine/config_loader.py`)

- Replace `_ensure_schema` with the §4 schema + **hard reset on `user_version`
  mismatch** (D1). Drop `mapping_id/mapping_version/enrichment_id/
  enrichment_version` from `channels`; add `pipeline`; retire `semantics`.
- Replace `save_mapping` / `save_enrichment` (and the would-be `save_filter` /
  `save_assert`) with **one generic set**:
  - `save_shared_step(kind, id, config, description="", version=None) -> int`
    — always inserts a new `(kind, id, version)` row (D3 immutability); never
    updates an existing row.
  - `list_shared_steps(kind=None) -> list[dict]`
  - `_shared_step_exists(kind, id, version) -> bool`
  - `_next_version(table, id)` / `_get_shared_step(kind, id, version)` — a
    version collision under concurrent edits surfaces as a typed
    "edited elsewhere, reload" error, not a raw `IntegrityError`.
- `save_channel_definition`: validate, then persist `channels.pipeline`; write one
  `pipeline_steps` row per step under its `step_id` with next version (resolved
  snapshot + provenance).
- `validate_channel_definition` (replace mapping/enrichment ref checks) — per
  pipeline step:
  - `type ∈ {enrich, transform, filter, assert}`;
  - inline vs shared shape is well-formed; shared ref exists at pinned version;
  - **enrich**: `source_key_field` is a valid canonical path; table/column
    identifiers pass `is_valid_identifier`;
  - **transform**: each rule has valid `source`/`target` canonical paths and `fn`
    ∈ `nodes.transform.functions.REGISTRY`; no whole-object assignment to
    `Identifier` leaves;
  - **filter/assert**: `expression` passes `validate_expression`; `on_fail` ∈ the
    allowed set for the type (D10).
  - Keep existing transport/destination/codec/retry checks unchanged.
- `_row_to_config`: parse `pipeline`; resolve shared refs into concrete config;
  emit `config["pipeline"]` (resolved, ordered) for the runner.
- Builders: refactor `_build_enrichment` → `_build_enrich_step(config)`; add
  `_build_transform_step`, `_build_filter_step`, `_build_assert_step`,
  `_build_pipeline_steps(config) -> list[Step]`; `build_runner` passes `steps=`
  instead of `mapper=`/`enricher=`.
- `seed_defaults`: seed `shared_steps` (e.g. `kind=mapping, id=identity`) and a
  demo channel whose `pipeline` is a list (or empty).
- Impact on `api/ui/definitions.py`: switch from `list_mappings()`/`list_enrichments()`
  /`save_mapping()`/`save_enrichment()` to the generic `list_shared_steps(…)`/
  `save_shared_step(…)`.

**Acceptance:** `tests/test_config.py` passes under the new shape; inline save
bumps `pipeline_steps` versions under stable `step_id`; unknown refs/fields/fns
rejected; shared enrich/transform still resolve; editing a pinned `shared_steps`
row is impossible (only new versions can be inserted).

> **Done.** `engine/config_loader.py` rewritten (pipeline column,
> `shared_steps`/`pipeline_steps` tables, `PRAGMA user_version=1` hard reset,
> generic `save_shared_step`/`list_shared_steps` CRUD, fail-fast
> `validate_channel_definition`, resolved-pipeline `_row_to_config`,
> `build_runner(steps=…)`, `seed_defaults`); `tests/test_config.py` rewritten
> from scratch (~37 tests). The `api/ui/definitions.py` impact item is also
> done: legacy mapping/enrichment routes replaced by one generic
> `/ui/defs/steps/save` + shared-steps table.

### P3 ✅ — Runner (`engine/runner.py`)

- Replace `mapper`/`enricher` params with `steps: list[Step] | None`; keep
  `inbound_codec`/`outbound_codec`/`max_retries`/`base_backoff`.
- Refactor into:
  - `process_one()` — dequeue → `process_envelope(envelope)` → success/failure
    bookkeeping (existing dequeue/mark paths unchanged).
  - `process_envelope(envelope, *, deliver=True, destination=None)` — **pure
    core**: decode → validate → `for step in steps: self._run_step(...)` →
    post-validate → encode → (deliver if enabled).
  - `run_dry(envelope) -> DryRunResult` — `process_envelope(deliver=False)` with a
    no-op destination and **no queue mutation**; returns per-step outcomes.
- `_run_step(envelope, step)` dispatch + error annotation (`step_id`, `step_type`):
  - **enrich** → `BatchLookup.enrich(envelope)` (read-only); wrap as `EnrichmentError`.
  - **transform** → `FieldMapper(...).transform(dict, lookups)` →
    `CanonicalMessage.model_validate`; wrap as `TransformError`/`CanonicalValidationError`.
  - **filter** → evaluate; on `false`, route per `on_fail` (`dead_letter`/`discard`,
    D10), raise `FilterError(retryable=False, context={"action": ...})`.
  - **assert** → evaluate; on `false`, raise `AssertError(retryable=(on_fail=="retry"))`.
- `_handle_failure`: `retryable = exc.retryable if exc.retryable is not None else
  exc.stage not in PERMANENT_STAGES`; record `step_id`/`step_type` in the error
  dict; honor `action == "discard"` → `queue.mark_discarded(...)` (D9).
- Add `PersistentQueue.mark_discarded(trace_id, attempts, error)` + `discarded`
  audit event (D9) — distinct from `mark_dead_letter`, so DLQ counts and
  discard counts never merge.

**Acceptance:** existing `tests/test_runner.py` outcomes preserved; new tests for
ordered multi-enrich, filter→DLQ/discard, assert→retry/DLQ, error carries
`step_id`/`step_type`, `run_dry` doesn't touch the queue, and DISCARDED messages
never increment the DLQ count used by `channel_health`.

> **Done.** `engine/runner.py` rewritten (step-chain executor, per-step
> `retryable` honoring `on_fail`, discard → `mark_discarded`, `run_dry` with
> per-step outcomes — including the fix that assigns each step's returned
> canonical back to the dry-run envelope so transforms are visible in previews);
> new `engine/steps.py` (`Step` dataclass + `build_step` factory).
> `tests/test_runner.py` (17) and `tests/test_enrichment.py` (29) rewritten for
> the step-chain shape.

---

### P4 — UI backend (`api/ui/channels.py` + optionally `api/ui/fields.py`)

> **Partially done (P2 cleanup):** the legacy paths are already gone —
> `channels.py` no longer builds mapping/enrichment selects and instead passes
> `shared_steps` + a `pipeline` JSON field through both `_render_channel_form`
> and `save_channel` (`_pipeline_from_request()` with fail-fast type checks);
> `channel_form.html` Step 2 renders a pipeline JSON textarea plus the
> available-shared-steps list; `definitions.py` serves the generic shared-step
> CRUD. This is an interim JSON-editor stopgap, verified end-to-end (shared
> step save → channel save with pinned `shared_ref` → resolved pipeline →
> invalid step type rejected). The structured builder below (field catalog,
> `api/ui/fields.py`, preview endpoints) is still open.

- `_render_channel_form`: stop building `mappings`/`enrichments` select lists;
  pass instead:
  - `pipeline` (resolved steps for editing),
  - `field_catalog = catalog_for_codec(inbound_codec)` (Source-tab picker +
    Pipeline-tab condition/mapping dropdowns),
  - step-type metadata (badge labels, per-type `on_fail` options per D10).
- `save_channel`: parse the ordered pipeline (JSON `<input name="pipeline">`
  assembled by JS) and pass through to `save_channel_definition`; drop
  `mapping_id`/`enrichment_id` handling.
- New HTMX endpoints (recommend a new `api/ui/fields.py` blueprint to keep
  `channels.py` focused):
  - `GET /ui/fields/<codec>` → `{fields:[{path,label,kind,group}]}` (honoring
    codec-key casing).
  - `POST /ui/preview` → parse a sample via the **same codec/decode path**
    (`DecodeError` returns typed errors, never a traceback); return a field tree.
  - `POST /ui/enrichments/columns` → `adapter.column_names(table)`, guarded by
    `is_valid_identifier`.
- Register the new blueprint in `api/app.py`; add `fhir.r4` to `helpers.codec_label`.

**Acceptance:** single-screen authoring; friendly labels everywhere; canonical
paths never hand-typed; preserve the SSTI-safe single-`render_template()` rule in
`_render_channel_form`.

### P5 — UI frontend (`api/ui/templates/channel_form.html` + `api/static/engine.css`)

- **Source tab:** add the "Available Fields" picker panel (field chips with
  `data-path` + human label + optional codec hint). Chips insert the canonical
  path into whichever step input has focus.
- **Pipeline tab:** replace the Mapping/Enrichment selects with a vertical
  **step-chain**:
  - collapsible `step-card`s (enrich/transform/filter/assert badges),
  - `+ Add Step` menu (4 types),
  - mapping-row builder table (`Source Field → Target Field → Function → ✕`,
    `+ Add Mapping`),
  - filter/assert condition rows (`Field / Condition / Value` → JSONLogic),
  - per-type `On Failure` selects — **filter**'s "Route to review queue" option
    and **assert**'s "Send to dead letter" option must serialize to the same
    `dead_letter` value (D10); only `on_fail=discard` (filter-only) is distinct,
    and only DISCARDED (never DLQ) on the backend,
  - `⋮` menu with "Promote to shared definition" (kind selector lands in
    `shared_steps`) + reorder/delete.
- **JS:** step add/remove/reorder/toggle/menu + JSON assembly into a hidden
  `pipeline` field (each step carries its `step_id`); field-chip injection;
  dynamic on-fail selects.
- **CSS:** port `step-card`, `step-badge`, `field-chip`, `mapping-table`,
  `add-menu`, `step-scope` from the mockup into `engine.css`.

**Acceptance:** matches the mockup UX; produced JSON is byte-compatible with the
new runner/config contract; values escaped via `_esc`; no user text re-parsed as a
template; no UI path can produce a third `on_fail` value beyond D10's enum.

---

### P6 — Tests, reset, cleanup

- ✅ New: `tests/test_expression.py`, `tests/test_field_catalog.py` (done in P1).
- ✅ `tests/test_config.py`, `tests/test_runner.py`, `tests/test_enrichment.py`
  rewritten for the step-chain shape (P2/P3; 244 tests passing). Remaining:
  extend `tests/test_errors.py` with step-identity/retryability cases if gaps
  appear, and add UI-level tests once P4/P5 land.
- Regression: `tests/test_pipeline_formats.py`, `tests/test_canonical_paths.py`,
  codec tests unchanged.
- Update `ARCHITECTURE.md` §19 (schema) + pipeline flow; cross-ref
  `FIELD_MAPPING.md`; mark `docs/ui_field_picker.md` implemented.
- Reset: `rm queue.db` (or rely on the `user_version` hard reset) and re-seed.

---

## 9. Dependency order

```text
P0 (errors) ✅ ─► P1 (expression + catalog) ✅ ─► P2 (schema/config) ✅ ─► P3 (runner) ✅
                                                                      │
P4 (UI backend) ⏳ ─► P5 (UI frontend) ─► P6 (tests/reset)  ◄──────────┘
```

P0–P3 complete — 244 tests passing, zero regressions. P4 is next: the field
catalog / preview endpoints and the structured step-chain builder
(`api/ui/fields.py`, form rework). The legacy-route removal and interim pipeline
JSON editor in `channels.py`/`definitions.py` are already done (see P4 note).

---

## 10. Open decisions

**None remaining.** All decisions are locked (D1–D10, §3). Proceeding from P4.

---

## 11. Acceptance criteria / test matrix

| Area | Test |
|---|---|
| Errors ✅ | `context` carries `step_id`/`step_type`; `retryable` honored; `FilterError`/`AssertError` stages |
| Expression ✅ | unknown op rejected; unknown `var` field rejected; operator semantics correct; no `eval`/`exec` |
| Field catalog ✅ | label ↔ path round-trips; `Identifier` leaf granularity; codec-aware hints |
| Config ✅ | pipeline validation fail-fast; inline auto-version under stable `step_id`; shared ref existence; generic `shared_steps` CRUD; `shared_steps` rows never mutate in place |
| Runner ✅ | ordered multi-enrich; filter → DLQ/discard; assert → retry/DLQ; error carries step identity; `run_dry` is side-effect-free; DISCARDED never counted as DLQ |
| UI | step-chain builder emits valid pipeline JSON; friendly labels only; SSTI-safe single render; filter/assert `on_fail` selects never emit a third state |

---

## Appendix — file change checklist

- `core/errors.py` ✅ — new kwargs + `FilterError`/`AssertError`
- `core/message.py` ✅ — `MessageState.DISCARDED`
- `core/queue.py` ✅ — `mark_discarded()` + `discarded` audit
- `core/expression.py` ✅ *(new)*
- `core/field_catalog.py` ✅ *(new)*
- `engine/config_loader.py` ✅ — schema, generic CRUD, validation, builders
- `engine/runner.py` ✅ — step-chain executor + `run_dry`
- `engine/steps.py` ✅ *(new)* — `Step` dataclass + `build_step` factory
- `api/ui/channels.py` ⏳ — legacy routes removed + pipeline JSON field done; field-catalog context pending
- `api/ui/fields.py` — `/ui/fields/*`, `/ui/preview`, columns
- `api/ui/definitions.py` ✅ — generic `shared_steps` CRUD
- `api/ui/helpers.py` — `fhir.r4` codec label
- `api/ui/templates/channel_form.html` ⏳ — pipeline JSON editor + shared-steps list done; step-chain builder + field picker pending
- `api/static/engine.css` — port mockup styles
- `api/app.py` — register new blueprint
- `tests/*` — `test_errors.py` ✅, `test_queue.py` ✅, `test_expression.py` ✅, `test_field_catalog.py` ✅, `test_config.py` ✅ (rewritten), `test_runner.py` ✅ (rewritten), `test_enrichment.py` ✅ (rewritten)
- `ARCHITECTURE.md`, `FIELD_MAPPING.md`, `docs/ui_field_picker.md` — docs