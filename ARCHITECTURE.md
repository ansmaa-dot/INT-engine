# INT-engine — System Architecture

An **integration / interface engine** for health and lab messaging
written in Python. It is sync/thread-based throughout (no asyncio):
declaratively configured **channels** stored in SQLite (each an inbound
transport + codec, an ordered step chain, an outbound codec, and a
destination), a persistent SQLite-backed message queue, a background
**worker daemon**, and a
**Flask + HTMX** control plane. It consumes and produces several wire formats —
including **HL7 v2.5.1** and **FHIR R4** — but every format is normalized
through a single, transport-independent **canonical model**.

---

## 1. At a glance

| Concern | Where | What |
|---|---|---|
| Wire format ↔ Canonical model | `nodes/codec/` | JSON, passthrough, HL7 v2 (ORU^R01 / ADT^A01), FHIR R4 |
| Canonical (pipeline currency) | `core/model/base.py` | `CanonicalMessage` + patient / order / specimen / observations |
| Persistent queue + audit | `core/queue.py` | SQLite (WAL), atomic multi-worker dequeue, idempotency / backpressure / audit |
| Queue & pipeline messages | `core/message.py`, `core/transport.py` | `Envelope`, `TransportMessage`, `DestinationMessage` |
| Ingress | `nodes/ingestion/` | http_webhook, mllp, http_poller, file_watcher, db_poller |
| Enrichment | `nodes/enrichment/` | `BatchLookup` (batched reference-data lookup) |
| Transform | `nodes/transform/` | `FieldMapper` + whitelisted functions |
| Egress | `nodes/destination/` | http, mllp, sftp |
| Auth | `core/auth_manager.py` | api_key / basic / bearer / OAuth2 client-credentials |
| Config registry | `engine/config_loader.py` | declarative channels + versioned reusable definitions |
| Worker daemon | `engine/main.py` | per-channel workers, ingestion lifecycle, stuck-claim reclaim |
| Control plane / UI | `api/app.py` → blueprints in `api/ui/` | dashboard, tabbed channel editor, DLQ, audit trail, message inspector |

## 2. Running it

```bash
pip install -r requirements.txt
cp config/auth_profiles.json.example config/auth_profiles.json   # add real secrets

# two processes
python api/app.py          # Web UI + admin API + webhook receiver, :5000
python -m engine.main      # worker daemon: drains queues, runs pollers/MLLP/file watcher
```

Dependencies (`requirements.txt`): **flask, requests, SQLAlchemy, paramiko,
pydantic, pytest**.

## 3. Repository layout

```
api/
  app.py                         # Flask app factory, blueprint registration, root/health
  deps.py                        # Shared singletons (queue, registry, auth, webhooks)
  api_routes.py                  # Blueprint: /api/ingest, /api/config/validate, /ui/test/simulate
  ui/
    __init__.py
    helpers.py                   # UI utilities: escape, ago, payload/error formatting, channel_health
    channels.py                  # Blueprint: /ui/metrics, /ui/channels/*, form renderer, save
    definitions.py               # Blueprint: /ui/defs/* (mappings, enrichments, retry policies)
    messages.py                  # Blueprint: /channels/<id>/messages/* (browse + split inspector)
    dlq.py                       # Blueprint: /ui/dlq/*, /ui/audit, /api/audit/*
    templates/
      index.html                 # dashboard (channels, metrics, DLQ, audit lookup, editor tabs)
      channel_form.html          # tabbed 4-step channel editor (Basic → Source → Pipeline → Destination)
      channel_messages.html      # per-channel messages + split Source|Destination inspector
config/
  auth_profiles.json(.example) # API keys/tokens (NEVER stored in channel config)
core/
  model/base.py               # CanonicalMessage domain model (pydantic)
  message.py                  # Envelope + MessageState
  queue.py                    # PersistentQueue (SQLite WAL)
  transport.py                # TransportMessage / DestinationMessage contracts
  wire.py                     # WireContext (provenance handed to codec.parse)
  errors.py                   # typed pipeline error taxonomy (stage + code)
  canonical_paths.py          # dotted-path resolve/assign over the canonical model
  auth_manager.py             # auth profiles + cached/locked OAuth token refresh
engine/
  config_loader.py            # ChannelConfigRegistry: schema, validation, builders
  main.py                     # worker daemon
  runner.py                   # ChannelRunner: the canonical pipeline, one message at a time
nodes/
  base.py                     # IngestionNode / EnrichmentNode / TransformNode / DestinationNode
  codec/
    base.py                   # Codec ABC (parse / serialize)
    registry.py               # exact-key codec registry (no implicit fallback)
    __init__.py               # registers all built-in codecs
    json.py / rawjson.py / passthrough.py
    hl7v2/{codec,parser,serializer,er7}.py
    fhir/codec.py
  ingestion/   http_webhook, mllp_server, http_poller, file_watcher, db_poller
  enrichment/  batch_lookup
  transform/   field_mapper, functions
  destination/ http_client, mllp_client, sftp_client
tests/                         # ~110 pytest tests covering passes 1-5
  fixtures.py                  # golden HL7 + FHIR fixtures
queue.db                       # SQLite database (queue + audit + channels + defs)
```

## 4. High-level dataflow

```
 Inbound transports (webhook / MLLP / http_poller / file_watcher / db_poller)
      |  TransportMessage { raw bytes|text, source, idempotency hint }
      v
 Envelope.raw  ------> PersistentQueue (SQLite, WAL)
                          |  dequeue_available()   (atomic, worker-safe claim)
                          v
        +-----------------------------------------------------------+
        |  ChannelRunner.process_one()  — the canonical pipeline      |
        |    decode -> validate -> enrich -> transform               |
        |          -> validate(post-transform) -> encode -> deliver  |
        +-----------------------------------------------------------+
      Inbound codec            |    Outbound codec
      (JSON/HL7/FHIR/...)     |    (JSON/HL7/FHIR/...)
      Wire -> Canonical        v    Canonical -> Wire
   Destination (http / mllp / sftp)  <-- receives ALREADY-SERIALIZED content

 Everything between decode and encode works on CanonicalMessage only.
```

## 5. Core concepts

### 5.1 Canonical message (the pipeline currency)

Defined in `core/model/base.py` (pydantic). Every codec decodes *into* it and
every outbound codec serializes *from* it; the queue, enrichment and mapper all
operate on it, never on a transport-specific shape.

```python
CanonicalMessage
├── schema_version: "1.0"
├── patient:     PatientSummary   # identifiers[Identifier], name, dob(date), gender
├── encounter:   Encounter         # identifiers, visit_number(Identifier), started_at
├── order:       Order             # identifiers, accession, requested_at, priority,
│                                  #   ordering_provider, items[OrderItem{code, ...}]
├── specimen:    list[Specimen]     # identifiers, type, collected_at
├── observations: list[Observation] # identifiers, code(LOINC), status, value, unit,
│                                   #   reference_range, observed_at, extensions{}
├── metadata:    MessageMetadata   # format, version, profile, message_type, source,
│                                 #   message_id, received_at
└── extensions:  dict              # explicit escape hatch for vendor data
```

Design rules enforced by the codecs:

- **Identifiers are typed** (`Identifier{system, value, type}`), never bare
  strings — so a LOINC code, an MRN and a placer id stay distinguishable and
  lossless across formats (e.g. HL7 ↔ FHIR).
- **`extensions` is the only escape hatch** for vendor-specific data that has
  no typed field (e.g. HL7 **OBX-8 abnormal flags** are parsed into
  `observations[].extensions["abnormal_flags"]` and re-emitted as FHIR
  `Observation.interpretation`) — nothing is silently dropped on a
  parse/serialize round trip.
- Fields are optional by default; a codec fills only what the source expressed.
- No pipeline/queue runtime state lives in the model (that is `Envelope` +
  queue state).

### 5.2 Canonical paths

`core/canonical_paths.py` — the normal transformation contract:
`resolve_path(data, "patient.identifiers.0.value")` and
`assign_path(dict, "patient.name", v)`. Paths traverse dicts and lists
(integer segments index lists) but there is **no `raw.*` business namespace**:
raw wire content is available only for audit/replay/debugging, never as the
transformation interface.

`unflatten_dot_keys(payload)` folds flat dot-notation keys back into the
nested tree — the shape-normalization primitive for **schemaless** inbound
(see §7.3): `{"patient.name": "Ada", "patient.identifiers.0.value": "42"}`
→ `{"patient": {"name": "Ada", "identifiers": [{"value": "42"}]}}`.

### 5.3 Envelope and message state

`core/message.py`:

- `Envelope` — carries `channel_id`, `raw` (original wire bytes, audit only),
  `inbound_codec`, `canonical`, `lookups` (enrichment results), plus
  `trace_id`, `state`, `attempts`, `error`, `idempotency_key`, `created_at`,
  `next_retry_at`.
- `MessageState` — `QUEUED`, `PROCESSING`, `DELIVERED`, `DEAD_LETTER`.

### 5.4 Transport/Destination contracts

`core/transport.py`:

- `TransportMessage` — raw content + `source`, optional `message_id` (an
  idempotency hint), `filename`, `content_type`. Transports **never**
  interpret message formats.
- `DestinationMessage` — already-serialized `content` (str/bytes) + delivery
  metadata. Destinations **never** serialize.

### 5.5 WireContext

`core/wire.py` — process/provenance context handed to a codec's `parse()`:
`source`, `message_id`, `received_at`, `content_type`. Separate from the
domain `CanonicalMessage`.

### 5.6 Error taxonomy

`core/errors.py` — every pipeline failure is a `PipelineError` subclass carrying
a stable `code` and a `stage`, so the audit/UI classify failures precisely:

```
wire        — malformed at the transport boundary
decode      — wire parsed but codec couldn't produce canonical
validation  — decoded data invalid per the canonical model
business    — semantically invalid business data
transform   — mapping/transform step failed
serialize   — canonical couldn't be expressed as the target format
destination — transport/delivery failed
```

Example codes: `hl7.no_msh`, `hl7.malformed`, `fhir.invalid_json`,
`fhir.not_bundle`, `json.invalid`, `canonical.invalid`, `codec.not_found`,
`transform.failed`, `destination.failed`. Each carries optional `context` and
`cause`.

## 6. The canonical pipeline (`ChannelRunner`)

`engine/runner.py` — `ChannelRunner.process_one()` runs one message through a
staged pipeline. **Only `CanonicalMessage` crosses stage boundaries.**

```text
dequeue_available()  → 1. decode    (inbound codec: wire → Canonical)
                        2. validate  (schema conformance)
                        3. enrich    (optional BatchLookup → env.lookups)
                        4. transform (optional FieldMapper, fed from canonical JSON)
                        5. validate  (post-transform schema)
                        6. encode    (outbound codec: Canonical → wire str)
                        7. deliver   (destination gets DestinationMessage{str/bytes})
                        8. mark DELIVERED + audit
```

Failure classification (`PERMANENT_STAGES = decode, validation, business,
transform, serialize`):

- **Permanent** stages → straight to `DEAD_LETTER` (no retry).
- **Retryable** (`destination`, unexpected) → requeued with exponential backoff
  (`delay = base_backoff * 2^(attempt-1)`) until `attempts >= max_retries`, then
  DLQ.

Errors are recorded with `{stage, code, message, traceback, trace_id, channel_id}`
and written to the queue row + `audit_log`.

## 7. Codec registry and wire formats

### 7.1 Registry (`nodes/codec/registry.py`)

- `register(codec)` keyed by `codec.key`; `get(key)` resolves **exactly** —
  an unknown/misspelled key raises `CodecNotFoundError`; **there is never an
  implicit passthrough fallback**.
- All built-in codecs are registered by `nodes/codec/__init__.py`:

| key | format | structure |
|---|---|---|
| `json` | JSON ↔ canonical (strict, canonical keys only) | structured |
| `schemaless.json` | arbitrary JSON incl. flat dot-notation (`rawjson.py`) | schemaless |
| `passthrough` | raw text preserved verbatim in `extensions._passthrough_raw` | structured |
| `hl7v2.5.1.ORU_R01` | HL7 v2.5.1 results messages | structured |
| `hl7v2.5.1.ADT_A01` | HL7 v2.5.1 admit/discharge messages | structured |
| `fhir.r4` | FHIR R4 JSON Bundle | structured |

### 7.2 JSON (`nodes/codec/json.py`)

- `parse`: bytes/str → `json.loads` → `CanonicalMessage.model_validate`.
  Malformed JSON → `DecodeError(json.invalid)`; valid JSON but invalid model →
  `CanonicalValidationError(canonical.invalid)`. Must be a JSON **object**.
- `serialize`: `model_dump(mode="json")` with `sort_keys=True` so re-parsing
  yields an equal canonical message.

Because Pydantic ignores extra keys by default, the strict `json` codec also
*ignores* flat dot-notation keys — `{"patient.name": "John"}` decodes and
validates with `patient=None`, so an outbound HL7 PID silently builds from
nothing. Use `schemaless.json` for that shape (below).

### 7.3 Schemaless / raw JSON (`nodes/codec/rawjson.py`)

For **schemaless inbound transports** (`db_poller`, `http_webhook`,
`http_poller`) whose raw JSON has no fixed schema — including flat
dot-notation rows like `{"patient.name": "John", "patient.identifiers.0.value":
"123"}`. This codec is the shape-normalization boundary:

1. bytes → `json.loads` (same `json.*` error taxonomy).
2. `unflatten_dot_keys()` folds flat dotted keys into nested dicts/lists
   (via the same `assign_path` used by the mapper).
3. Canonical top-level groups (`patient`, `encounter`, `order`, `specimen`,
   `observations`, `metadata`, `schema_version`, `extensions`) validate into
   the model; **any other inbound key is preserved under `extensions.<path>`**
   — nothing is silently dropped.
4. `CanonicalMessage.model_validate`.

`serialize` emits the normalized canonical JSON (identical to `json`), so
round-tripping a schemaless channel is lossless. Missing transport provenance
is filled from `WireContext` when the payload carries no `metadata` block.

A channel using `schemaless.json` plus dataless sources still gets a *valid*
(empty) canonical message; the config layer surfaces a **non-fatal advisory
hint** (`ChannelConfigRegistry.channel_shape_hints`, UI: yellow note beside the
codec select, API: `hints` array in `/api/config/validate`) when a schemaless
transport is paired with a structured codec or vice versa.

### 7.4 Passthrough (`nodes/codec/passthrough.py`)

Explicit-only codec for legacy/simple channels. Stores the original wire text
in `extensions["_passthrough_raw"]`; `serialize` returns it unchanged.

### 7.5 HL7 v2.5.1 (`nodes/codec/hl7v2/`)

- **`er7.py`** — low-level ER7 primitives: segmentation, component/sub-component
  splitting, and HL7 escaping (`\F\`=^, `\S\`=`|`, `\T\`=&, `\R\`=~, `\E\`=\\).
- **`parser.py`** — decodes ORU^R01 / ADT^A01 into the canonical model:
  MSH (version, message type, control id, source), PID (identifiers incl.
  `~`-repeated CX, name parsed as `given family`, DOB→`date`, gender), PV1
  (visit number), ORC/OBR (placer/filler, accession, requested-at, priority,
  ordering provider, test code), SPM (id, type text/code, collected-at), OBX
  (code, value with numeric coercion, unit, reference range, **OBX-8 abnormal
  flags → `extensions`, OBX-11 status, OBX-14 observed-at**).
- **`serializer.py`** — the inverse. Index convention: **segment field N → list
  index N** (index 0 = segment name; MSH is the exception: index 0 = the
  separator field, so MSH-12 → index 11).
- **`codec.py`** — the thin `Hl7V2Codec(profile, key, version)` wrapper used for
  the two registered profiles.

Round-trip-safe: escaping/unescaping, `date` handling (`1990-01-02` not
midnight), and OBX-8 flags survive canonical → HL7 → canonical.

### 7.6 FHIR R4 (`nodes/codec/fhir/codec.py`)

Maps canonical → a JSON **Bundle** of `Patient`, `ServiceRequest`, `Specimen`,
`Observation`, and back:

- Bundle `id` = `metadata.message_id` (provenance follows the message).
- Numeric observation values → `Observation.valueQuantity` (keeps `unit`);
  string values → `valueString`.
- `extensions.abnormal_flags` ↔ `Observation.interpretation` (lossless).
- Priority/status use explicit invertible maps
  (e.g. canonical `preoperative` ↔ FHIR `urgent`;
  canonical `unable_to_obtain` ↔ FHIR `unknown`).
- `parse` requires a `Bundle` (else `DecodeError(fhir.not_bundle)`); invalid
  JSON → `DecodeError(fhir.invalid_json)`.

**End-to-end paths proven by tests:**
`HL7 v2 → canonical → FHIR R4 Bundle`, and `FHIR R4 Bundle → canonical → HL7 v2`,
including a full `ChannelRunner` delivery with inbound `hl7v2.5.1.ORU_R01` and
outbound `fhir.r4`.

## 8. Ingestion transports (`nodes/ingestion/`)

All transports are **format-agnostic**: they only frame/collect raw content and
emit a `TransportMessage`. Parsing happens later in the codec stage. All support
optional **idempotency** and **`max_queue_depth` backpressure**.

| Transport | File | Notes |
|---|---|---|
| http_webhook | `http_webhook.py` | Flask blueprint, `POST /webhooks/<channel_id>`. Optional HMAC-SHA256 signature header; returns `202 accepted` / `503` when at capacity / dedupes. Lives on the Flask process. |
| mllp | `mllp_server.py` | Raw TCP MLLP listener (`VT … FS CR` framing). **Enqueues (persists) before ACKing** so a crash never loses an ACKed message; NACKs (`AE`) at capacity; emits `AA`/echoes MSH-10 control id. One thread per connection with an idle timeout + a connection semaphore. |
| http_poller | `http_poller.py` | Interval GET of a REST endpoint; enqueues the raw response body (content-type preserved). Auth via `AuthManager` profile. |
| file_watcher | `file_watcher.py` | Polls a directory for `.csv/.hl7/.txt`; **claims a file by renaming to `.processing`** (crash-safe: leftovers are re-queued on restart) then moves it to `processed/` or `failed/`. Reads with `newline=""` so `\r` (HL7 segment separators) are preserved. |
| db_poller | `db_poller.py` | Polls a DB (SQLite/PostgreSQL/MySQL via SQLAlchemy) on an interval; each row enqueued as JSON. Optional cursor-based incremental pulls via `cursor_field`/`cursor_param`, and an `idempotency_key_field`. |

The schemaless transports (`http_webhook`, `http_poller`, `db_poller`) now
default their **inbound codec to `schemaless.json`** so flat dot-notation rows
unflatten into a nested canonical tree before outbound encoding. `mllp` /
`file_watcher` carry raw wire text (HL7 / CSV) and pair naturally with
structured codecs (`hl7v2.*`, `fhir.r4`).

## 9. Enrichment (`nodes/enrichment/batch_lookup.py`)

`BatchLookup` resolves reference data (patient name, doctor id, test code…) for a
**whole batch** of envelopes in one SQL `WHERE … IN (…)` query — avoiding the
per-message N+1 lookup pattern. Results attach to `env.lookups[lookup_name]`
(a missing match is stored as `None`, so a downstream `required` mapping field
catches and DLQs it). Lookup keys are canonical paths into the message
(`source_key_field`), never transport shapes.

## 10. Transform (`nodes/transform/`)

- `field_mapper.py` — `FieldMapper(mappings)` where a mapping is
  `{source, target, required?, fn?, fn_args?}`. Sources are canonical paths,
  `lookups.<name>.<field>`, or `extensions.*` (inbound data preserved by the
  schemaless codec); targets are canonical paths (or `extensions.*`) written
  back onto a copy of the message so untouched fields survive. The mapped
  result is re-validated into a `CanonicalMessage`. Each rule may apply a
  whitelisted function from `nodes/transform/functions.py`
  (`Uppercase`, `Lowercase`, `Trim Whitespace`, `Format`, `Default`) plus
  JSON `fn_args`; the channel-editor Map step exposes both (Function dropdown
  and a context-sensitive Args box that turns into a date-format field for
  `Format` and a fallback-value field for `Default`) in sync with the backend
  registry.
- `functions.py` — **whitelisted** transform functions only (no `eval`/scripting):
  `Uppercase`, `Lowercase`, `Trim Whitespace`, `Format` (date),
  `Default`. Unknown names fail loudly at config time.

## 11. Destinations (`nodes/destination/`)

Destinations receive an **already-serialized** `DestinationMessage` (str/bytes);
they never serialize. Invalid content (a dict) is rejected with `TypeError`.

| Destination | File | Behavior |
|---|---|---|
| http | `http_client.py` | POST/PUT to an endpoint with content-type + optional auth profile; on `401` invalidates the cached token and retries **once** with a fresh token. |
| mllp | `mllp_client.py` | Frames content with MLLP start/end blocks, sends over TCP, and waits for an ACK. |
| sftp | `sftp_client.py` | Uploads the payload as a file over SFTP; password or private-key (`.ppk`/PEM) auth; filename from delivery metadata or a generated default. |

## 12. Auth manager (`core/auth_manager.py`)

Credentials live **only** in `config/auth_profiles.json` (git-ignored), never in
channel config — channel rows reference an `auth_profile_id`. Supported types:
`none`, `api_key`, `basic`, `bearer_static`,
`oauth2_client_credentials` (with cached + per-profile-locked token refresh so a
burst of expired tokens doesn't each fire a refresh call). `invalidate()` lets
an HTTP destination drop a stale token and retry once.

## 13. Queue and reliability (`core/queue.py`)

`PersistentQueue` backs everything with SQLite in **WAL mode** (safe for
concurrent workers/processes). Tables: `queue` (current state),
`idempotency_keys`, `audit_log` (append-only lifecycle history).

- **Atomic claim** — `dequeue_available()` claims a row with a single
  `UPDATE … WHERE trace_id = (SELECT …)` plus a per-call claim token, and reads
  back exactly what it claimed, so concurrent threads/processes never double- or
  under-claim (verified under 8 racing threads).
- **Idempotency** — `UNIQUE(channel_id, idempotency_key)`; duplicate keys for a
  channel are rejected at enqueue (no check-then-insert race). Keys come from an
  `idempotency_key_field` on the transport, or MSH-10 automatically for MLLP.
- **Backpressure** — each transport honors `max_queue_depth` without dropping:
  HTTP poller skips its tick, webhook returns `503`, MLLP NACKs (`AE`), file
  watcher leaves the file in place, db poller skips.
- **Audit trail** — every `queued`, `processing_started`, `delivered`,
  `retry_scheduled`, `dead_lettered`, `duplicate_rejected` event is appended to
  `audit_log` (trace_id-indexed), independent of current-state rows.
- **Retry / DLQ** — see §6; `mark_retry` (with backoff + `next_retry_at`) and
  `mark_dead_letter` persist structured errors.
- **Crash recovery** — `reclaim_stale_processing(120s)` puts any `PROCESSING`
  row claimed longer than 2 minutes back to `QUEUED` (run every 30s by the
  worker daemon), so a worker that dies mid-flight doesn't strand a message.

## 14. Channel configuration (`engine/config_loader.py`)

A channel is **orchestration only**:

```text
Inbound Transport + Inbound Codec + [Enrichment ref] + [Mapping ref]
+ Outbound Codec + Destination + Retry Policy ref [+ optional semantics]
```

Reusable definitions live in dedicated, **versioned** tables:

- `mappings` — `(mapping_id, version)` → JSON rules (canonical paths).
- `enrichments` — `(enrichment_id, version)` → lookup definition.
- `retry_policies` — `retry_policy_id` → `{max_retries, base_backoff_seconds}`.

Editing a mapping/enrichment creates a **new version row**; channels pin a
specific version, so a change never silently alters consumers.

`ChannelConfigRegistry`:

- `validate_channel_definition()` is fail-fast — unknown transports, codecs,
  destinations, and missing references are rejected at save time (never a
  silent fallback, e.g. no implicit `passthrough` codec).
- `build_runner()` / `build_ingestion()` turn a validated config into live
  objects (used by both the worker daemon and tests).
- `seed_defaults()` seeds a demo channel (`his_to_lis`) and a `default` retry
  policy + `identity` mapping on first run.

## 15. Worker daemon (`engine/main.py`)

- Config-sync loop every 2s: for each enabled channel starts `concurrency`
  worker threads (`_worker_loop`), each draining the shared queue. Paused or
  disabled channels stop their workers and ingestion.
- Starts background **ingestion** nodes for `mllp` / `http_poller` /
  `file_watcher` / `db_poller` (webhooks live on the Flask app).
- Runs the **reclaim loop** every 30s (§13).

## 16. Control plane — application structure

The Flask control plane was refactored from a single `api/app.py` (~1100 lines)
into **domain blueprints** with a shared-singleton pattern:

```
api/app.py            (84 lines) — create_app() factory, blueprint registration, /, /health
api/deps.py           (47 lines) — module-level singletons (queue, registry, webhooks, sync_webhooks)
api/ui/helpers.py     (157 lines) — _esc, ago, codec_label, pretty_payload, error_*, channel_health
api/ui/channels.py    (367 lines) — Blueprint: /ui/metrics, /ui/channels/*, tabbed form, save
api/ui/definitions.py (178 lines) — Blueprint: /ui/defs/* (mappings, enrichments, retry policies)
api/ui/messages.py    (141 lines) — Blueprint: /channels/<id>/messages/*, split inspector
api/ui/dlq.py         (161 lines) — Blueprint: /ui/dlq/*, /ui/audit, /api/audit/*
api/api_routes.py     (62 lines)  — Blueprint: /api/ingest, /api/config/validate, /ui/test/simulate
```

**Shared singleton pattern:** `api/deps.py` holds module-level `queue`, `registry`,
`auth_manager`, and `webhooks` references. `create_app()` initialises them before
any blueprint is imported, so every blueprint can do `from api.deps import queue`
and always see the live instance (Python module singletons are mutated in-place).

## 17. Tabbed channel editor (`channel_form.html`)

The channel creation/edit form uses a **4-step wizard** rendered from a Jinja2
template (replacing ~200 lines of Python f-strings):

```
Step 1 — Basic      → Channel ID, Name, Status, Concurrency, Enabled
Step 2 — Source     → Inbound Transport dropdown + Codec + context-aware Connection fields
Step 3 — Pipeline   → Outbound Codec, Mapping ref, Enrichment ref, Retry Policy, Business Rules
Step 4 — Destination → Destination dropdown + context-aware Connection fields
```

**Context-aware fields:** Instead of generic JSON textareas, each transport and
destination type gets its own structured field group that appears/hides when the
dropdown changes. Definitions live in `_TRANSPORT_FIELDS` / `_DESTINATION_FIELDS`
dicts in `channels.py`; JavaScript toggles visibility and serialises only the
visible group into hidden JSON inputs before HTMX submit.

| Transport | Fields |
|-----------|--------|
| `http_webhook` | Shared Secret, Signature Header, Max Queue Depth, Idempotency Key |
| `http_poller` | Poll URL, Interval, Auth Profile, Max Queue Depth, Idempotency Key |
| `mllp` | Listen Host, Port, Max Connections, Idle Timeout, MSH-10 dedup |
| `file_watcher` | Directory, Poll Interval, Extensions, Max Queue Depth |
| `db_poller` | Connection String, SQL Query, DB Type, Interval, Cursor Field/Param |

| Destination | Fields |
|-------------|--------|
| `http` | Endpoint URL, HTTP Method, Auth Profile, Headers (JSON), Timeout |
| `mllp` | Host, Port |
| `sftp` | Host, Port, Username, Password, Private Key Path/Passphrase, Remote Dir, Timeout |

## 18. Dashboard & inspector CSS

The dashboard (`index.html`) includes all CSS classes needed by HTMX fragments
swapped into the Inspector panel — error banners, trace disclosure blocks,
pane layouts, message metadata rows, and codec badges — so nothing renders
unstyled regardless of whether the fragment came from the dashboard or the
standalone channel-messages page.

## 19. Persistence schema (SQLite, `queue.db`)

- `channels` — channel row + inbound/outbound codec + reusable-definition refs.
- `mappings` / `enrichments` / `retry_policies` — versioned reusable defs.
- `queue(trace_id PK, channel_id, state, attempts, raw, inbound_codec,
  canonical, error, created_at, next_retry_at, idempotency_key, claimed_at,
  claim_token)`.
- `idempotency_keys(channel_id, idempotency_key, trace_id, seen_at)` with
  `PRIMARY KEY(channel_id, idempotency_key)`.
- `audit_log(id, trace_id, channel_id, event, detail, at)` (+ trace index).

## 20. Tests

~110 pytest tests in `tests/` (+ `tests/fixtures.py` with golden HL7 / FHIR
fixtures). Coverage by area:

- **core** — canonical model, canonical paths, error taxonomy, JSON codec,
  queue (claim/idempotency/retry/DLQ/audit), transport contracts.
- **engine** — config validation, declarative runner, and the full
  `ChannelRunner` pipeline (decode → transform → validate → encode → deliver),
  permanent-vs-retryable classification, unknown-codec rejection.
- **Pass 5** — HL7 v2 parse (golden ORU/ADT), escaping round-trips, malformed →
  typed `DecodeError`; FHIR R4 bundle parse, lossless canonical round-trip,
  abnormal-flag preservation, priority/status inversion; cross-format e2e
  (`HL7 → FHIR`, `FHIR → HL7`, and a real `ChannelRunner` delivery).
- **codec registry** — exact-key resolution, typed `CodecNotFoundError`, keys.

Run: `python -m pytest -q`.

## 21. Pass-by-pass build summary

- **Pass 1 — Canonical model & JSON codec**: pydantic `CanonicalMessage`,
  `Identifier`, typed errors; `json` codec.
- **Pass 2 — Queue & reliability**: `Envelope`/`MessageState`,
  `PersistentQueue` (SQLite WAL, atomic claim, idempotency, audit, retry/DLQ).
- **Pass 3 — Transports, destinations, enrich, transform**: format-agnostic
  transports, serialize-only destinations, `BatchLookup`, `FieldMapper` +
  whitelisted functions, `WireContext`, `ChannelRunner`.
- **Pass 4 — Config & control plane**: `ChannelConfigRegistry` (declarative
  channels, versioned defs, fail-fast validation), `engine/main` daemon, Flask +
  HTMX dashboard (channels, DLQ, audit).
- **Pass 5 — HL7 v2 & FHIR R4 codecs**: `nodes/codec/hl7v2/` (ORU^R01 /
  ADT^A01) + `nodes/codec/fhir/`, registered as `hl7v2.5.1.ORU_R01`,
  `hl7v2.5.1.ADT_A01`, `fhir.r4`; golden fixtures + cross-format e2e tests.
- **UI update** — readable codec labels, HL7-aware payload rendering, and codec
  badges in the Source|Destination inspector.
- **UI refactor (Pass 6)** — `api/app.py` split into domain blueprints
  (§16), 4-step tabbed channel editor with context-aware per-transport/destination
  fields replacing generic JSON textareas (§17), missing CSS classes added to
  dashboard for inspector fragments (§18), Definitions tab added to dashboard.

## 22. Known limitations / next steps

- Config **hot-reload staging/replay** (a changed channel takes effect on the
  next 2s sync tick, with no dry-run against recent messages).
- Multi-**process** horizontal scaling (the queue is already safe for it —
  `python -m engine.main` can run twice — but there is no supervisor/orchestrator).
- FHIR codec covers the laboratory subset (Patient/ServiceRequest/Specimen/
  Observation in a Bundle), not all FHIR resources.