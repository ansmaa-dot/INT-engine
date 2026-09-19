# INT-engine

An integration / interface engine for health and lab messaging written in Python. **Channels** are declared in SQLite and edited via the web UI (no JSON config files) — each is an inbound transport + codec, an ordered step chain (`decode → validate → [enrich | transform | filter | assert]* → validate → encode → deliver`), an outbound codec, and a destination, all piped through a single transport-independent **canonical message model**. Pluggable **codecs** normalize wire formats (HL7 v2.5.1, FHIR R4, JSON) to and from that model. Sync/thread-based throughout (no asyncio) — SQLite-backed queue, atomic multi-worker-safe dequeue, a background worker daemon, and a Flask + HTMX control plane.

> **Deep dive:** [`ARCHITECTURE.md`](ARCHITECTURE.md) is the current, authoritative reference (pipeline details, codecs, schema, reliability). This README is the quick start.

## Project status

This is a personal/educational project, built to explore interface-engine architecture and HL7/FHIR integration patterns. It is **not production-ready** and is not affiliated with, validated against, or intended as a replacement for any certified health IT product.

- No formal security audit, clinical validation, or regulatory review (e.g. HIPAA, IEC 62304) has been performed.
- Use in any environment handling real patient data is **not recommended** without independent review and hardening.
- Provided as-is, for learning and demonstration purposes, with no warranty and no guarantee of fitness for a particular purpose.

This repository is shared for portfolio and demonstration purposes only. No license is granted for reuse, modification, or distribution.

## Setup

```bash
pip install -r requirements.txt   # flask, requests, SQLAlchemy, paramiko, pydantic, pytest
cp config/auth_profiles.json.example config/auth_profiles.json  # already done; edit with real secrets
```

`config/auth_profiles.json` is git-ignored. Channels reference a profile by `auth_profile_id` — the actual API keys/tokens never live in the channel config, only in this file.

## Running

Two processes:

```bash
python api/app.py       # dashboard + admin API + webhook receiver, port 5000
python -m engine.main   # background worker daemon: drains queues, runs pollers/MLLP server/file watcher
```

Open `http://localhost:5000` for the dashboard.

## Screenshots

The main control-plane dashboard — live channel health, queue metrics, and the configured-channels table, all re-rendered in place via HTMX:

![Integration Engine Dashboard](docs/screenshots/dashboard.png)

## Architecture

- **Canonical pipeline** (`engine/runner.py`, `ChannelRunner`) — every message flows `dequeue → decode → validate → step chain → validate → encode → deliver`, one message at a time. Only a `CanonicalMessage` crosses stage boundaries (`core/model/base.py`); raw wire text is kept for audit/debug, never used as the pipeline's working currency.
- **Step chain** (`engine/steps.py`) — a channel's `pipeline` is an ordered list of typed steps: `enrich` (read-only `BatchLookup`), `transform` (pure `FieldMapper`), `filter` (boolean expression → `dead_letter` / `discard`), and `assert` (boolean expression → `retry` / `dead_letter`). Reusable steps live in the Definitions UI.
- **Codecs** (`nodes/codec/`) — pluggable and **exact-match** (an unknown key raises; no implicit passthrough fallback). Keys: `json` (strict canonical), `schemaless.json` (arbitrary/dot-key JSON), `passthrough` (raw text), `hl7v2.5.1.ORU_R01` / `hl7v2.5.1.ADT_A01`, and `fhir.r4` — see [`ARCHITECTURE.md`](ARCHITECTURE.md) §7.
- **Ingestion** (`nodes/ingestion/`) — format-agnostic transports that only frame raw content (parsing is the codec's job): `http_webhook` (Flask, optional HMAC), `mllp_server` (raw TCP, enqueue-before-ACK, NACKs `AE` at capacity), `http_poller` (interval GET), `file_watcher` (atomic `.processing` claim per file), `db_poller` (SQLAlchemy, cursor-based incremental).
- **Enrichment** — `nodes/enrichment/batch_lookup.py`; optional read-only DB lookup, referenced in mappings as `lookups.<lookup_name>.<field>`.
- **Transform** — `nodes/transform/field_mapper.py` + whitelisted functions only (Uppercase, Lowercase, Trim Whitespace, Format, Default — no eval, no dynamic scripting).
- **Destination** — `nodes/destination/`: `http_client` (auth-aware, retries once on 401 with a fresh token), `mllp_client`, `sftp_client` (file upload; password or private-key auth).
- **Auth** — `core/auth_manager.py`: api_key, basic, bearer_static, oauth2_client_credentials with cached/locked token refresh. Shared by ingestion and destinations (webhook signature secret is separate).
- **Control plane** (`api/app.py` → blueprints in `api/ui/`) — Flask + HTMX dashboard: channel control (metrics, per-channel enable/disable), engine inspector (DLQ requeue/discard, message audit-trail lookup by trace_id), tabbed channel editor (Basic → Source → Pipeline → Destination), and a **Definitions** manager (shared mappings, enrichments, filters/asserts, retry policies). CSRF-protected forms.

## Reliability features

- **Idempotency** — set an `idempotency_key_field` on any ingestion type (or it's derived from MSH-10 automatically for MLLP). Duplicate keys for the same channel are rejected at `enqueue()` via a `UNIQUE(channel_id, idempotency_key)` constraint — no separate check-then-insert race.
- **Backpressure** — set `max_queue_depth` on any ingestion type. HTTP poller skips its tick, webhook returns `503`, MLLP NACKs (`AE`), file watcher leaves files in place — all without dropping anything, so the source system's own retry logic picks it back up once the queue drains.
- **Persistent audit trail** — every `queued` / `processing_started` / `delivered` / `retry_scheduled` / `dead_lettered` / `discarded` / `duplicate_rejected` event is appended to `audit_log`, independent of the `queue` table's current-state-only row. Look up by trace_id from the dashboard ("Message Audit Trail Lookup") or `GET /api/audit/<trace_id>` — works for delivered and still-queued messages too, not just DLQ.
- **Multi-worker scaling** — set "Worker Threads" (`concurrency`) per channel. Safe because `PersistentQueue.dequeue_available()` atomically claims a row (via a single `UPDATE ... WHERE trace_id = (SELECT ...)` plus a per-call claim token to read back exactly the row it claimed) before handing it to a worker — verified under 8 concurrent threads racing the same queue with zero duplicate or dropped claims.
- **Crash recovery** — if a worker dies between claiming a message (`PROCESSING`) and finishing it, that message would otherwise be stuck forever since no other worker will re-claim a `PROCESSING` row. `engine/main.py` runs a background loop calling `reclaim_stale_processing()` every 30s to put anything claimed longer than 2 minutes back to `QUEUED`.

## Still not implemented

- Config hot-reload staging/replay before a changed channel definition goes live (edits currently take effect on the next 2s config-sync tick, with no dry-run against recent messages first).
- True multi-**process** horizontal scaling — `concurrency` currently spins up threads within one `engine/main.py` process. The queue is already safe for multiple OS processes against the same `queue.db` (SQLite WAL + the same atomic claim logic), so running a second `python -m engine.main` instance works today, but there's no supervisor/orchestration for it.
- The FHIR codec covers the laboratory subset only (Patient / ServiceRequest / Specimen / Observation in a Bundle), not all FHIR resources.