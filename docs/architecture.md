# Architecture — `social_service`

Service-level architecture for the social graph and private-conversation
service of the Community Chat & Voice Platform. This document is the clean
source of truth for the service's responsibilities, data, flows, contracts,
and operational model. Library, framework, and code-layout choices live in
`docs/engineering_guide.md`.

---

## 1. Service responsibilities & boundaries

`social_service` owns the **private social graph** of the platform:

- **Friend requests** — send, review (incoming/outgoing), accept, decline, cancel.
- **Friendships** — the unordered pair record created by an acceptance; removed mutually by either side.
- **Private conversations** — 1:1 direct conversations (one per pair, reused, never deleted) and small group conversations (≤ 10 participants, creator + up to 9 friends).
- **Conversation participant lists** — this service is the **authority on who may read and post in a private conversation**. `messaging_service` enforces that access from `conversation_changed` (EVT-008) projections; `realtime_gateway` derives per-user conversation scopes from the same event.

Out of scope for this service (enforced by boundaries, not by omission):

- Messages, threads, reactions, attachments (→ `messaging_service`).
- Communities, channels, community membership (→ `community_service`). Group membership is independent of unfriending.
- Account, session, and token issuance (→ `identity_service`). This service verifies access tokens **locally** and never calls `identity_service` at request time.
- Notification delivery (→ `notification_service`, which consumes this service's events).
- User discovery: friend suggestions are a client-side concern over `identity_service`'s public lookup and shared community member lists; `GET /friends` returning an empty list is the supported empty state.

Internal component boundaries (logical, not a code layout):

| Component | Responsibility |
|---|---|
| **HTTP interface layer** | Request parsing, access-token verification, input validation at the boundary, error shaping (`{"error", "detail"}`), `Idempotency-Key` handling. |
| **Friendship domain service** | Friend-request lifecycle state machine and friendship create/remove rules; emits friend-request events. |
| **Conversation domain service** | Direct/group conversation creation, participation, rename rules; version bumping; emits conversation events. |
| **Repository layer** | All PostgreSQL access; transactional consistency between state changes and the event outbox. |
| **Event publisher** | Envelope construction, trace propagation, Kafka produce with retries. |
| **Probes & config** | `/healthz`, `/readyz`, 12-factor configuration validation. |

---

## 2. Synchronous request flows

Every flow below assumes an authenticated caller (local JWT verification; claims `sub` = user id). All authorization is single-tenant: a caller may only touch requests/friendships/conversations they are a party to, enforced by scoping every query on the caller's `sub`.

### 2.1 Friend request lifecycle

```
Client ── POST /friend-requests ──▶ social_service
            │ validate (recipient ≠ self; 422 self_request)
            │ conflict checks: friendship exists → 409 already_friends
            │                  pending request either direction → 409 request_pending
            │ INSERT friend_request (pending)   [+ Idempotency-Key replay check]
            ▼
         INSERT event_outbox (EVT-007, status=pending)  → commit → publish
```

- `GET /friend-requests?direction=` — read-only, cursor-paginated, filtered on sender/recipient by the caller.
- `POST /friend-requests/{id}/accept` — recipient-only; pending-only (non-pending → `409 request_not_pending`; not a party → `404`). Acceptance is **idempotent**: an already-accepted request returns the existing friendship. Commits: request → `accepted`, friendship insert, EVT-007 outbox row — one transaction.
- `POST /friend-requests/{id}/decline` — recipient-only, pending-only; request → `declined`; EVT-007 outbox row.
- `DELETE /friend-requests/{id}` — sender-only, pending-only; request → `cancelled`; EVT-007 outbox row.
- `DELETE /friends/{user_id}` — deletes the friendship row for both sides (`404 not_friends` if absent). No event is emitted; shared group conversations are unaffected.

State machine (single `status` column, forward-only): `pending → accepted | declined | cancelled`. No transitions out of terminal states.

### 2.2 Direct conversation (open or reuse)

```
Client ── POST /conversations/direct ──▶ social_service
            │ friend check → 422 not_friends; self → 422 self_request
            │ existing direct conversation for pair? ──yes──▶ 200 existing (no event)
            ▼ no
         INSERT conversation (direct) + 2 participant rows
         INSERT event_outbox (EVT-008, change=created)  → commit → publish → 201
```

The reuse check and the insert happen in one transaction with a uniqueness
guard (see §5) so two concurrent "open DM" requests converge on one
conversation; the loser returns the winner's conversation with `200`.

### 2.3 Group conversation lifecycle

- `POST /conversations/group` — creator + 1–9 friends; every participant must be the caller's friend; total ≤ 10; all-or-nothing (any violation → `422` and nothing is created). Duplicate ids are deduplicated. Commits conversation + participant rows + EVT-008 (`created`) atomically.
- `POST /conversations/{id}/participants` — caller must be a **current participant** (`403 not_a_participant`); added users must be the **caller's** friends (`422 not_friends`); addition must keep the total ≤ 10 (`422 group_full`); all-or-nothing. Existing participants in the submitted list are ignored. Commits participant inserts + conversation version bump + EVT-008 (`participants_changed`, full list + `added_user_ids`).
- `POST /conversations/{id}/leave` — caller's participation row gets `left_at` (not a participant → `404`); direct conversations reject with `422 group_only_operation`. Emits EVT-008 (`participants_changed`) with `removed_user_ids = [caller]`. The conversation is never deleted; the last participant leaving leaves an empty conversation.
- `PATCH /conversations/{id}` — current participants only; group-only; name 1–100 chars (`422 invalid_name`). Emits EVT-008 (`renamed`).
- `GET /conversations` — conversations where the caller has an active participation (`left_at IS NULL`), cursor-paginated.

### 2.4 Cross-cutting rules

- **Idempotency-Key** is honored on `POST /friend-requests`, `POST /conversations/direct`, `POST /conversations/group` (the creation endpoints a client may retry). Replay of the same key + same request returns the original result; a same-key different-body request is rejected with `409 idempotency_key_reuse`.
- **Cursor pagination** (`cursor`, `limit`, default 50, max 100) on all list endpoints; cursors are opaque, keyed on `(created_at, id)` for stable ordering.
- **Errors** are always `{"error": "<code>", "detail": "<human text>"}`. Codes are stable snake_case values matching the OpenAPI spec; internal failures surface as `500` with no stack trace or internal detail.
- **No user-existence verification.** Recipient/participant ids are trusted from the caller (resolved client-side via identity's public lookup). An id that does not exist simply never accepts or never sees the conversation — this is deliberate and keeps the service free of synchronous dependencies.

---

## 3. Asynchronous event flows (Kafka producer)

`social_service` **produces only** on topic `social_events`; it consumes nothing. Delivery is at-least-once; consumers deduplicate on `envelope.event_id` and apply changes idempotently on the carried version counters.

| Contract | Event type | Topic | Partition key | Change set | Consumers |
|---|---|---|---|---|---|
| EVT-007 | `friend_request_changed` v1 | `social_events` | `recipient_user_id` | sent / accepted / declined / cancelled (full status snapshot) | `notification_service` |
| EVT-008 | `conversation_changed` v1 | `social_events` | `conversation_id` | created / participants_changed / renamed (full current participant list + delta + `conversation_version`) | `messaging_service`, `realtime_gateway` |

**Transactional outbox.** Every state change and its event are committed in
the same PostgreSQL transaction (state row + `event_outbox` row); a separate
publisher loop reads the outbox and produces to Kafka, marking rows published.
This guarantees the event is never lost or emitted for an uncommitted state.
Duplicate publishing (producer retry after a lost ack) is safe: consumers
dedupe on `event_id`.

**Event emission rules.**

- A reused direct conversation publishes **no** event (nothing changed).
- `conversation_version` is bumped on every conversation mutation and is the consumers' staleness guard.
- Envelope: `correlation_id` groups one business flow — one correlation id per friend-request lifecycle and per conversation; the first event of a flow has `causation_id = null`, later events carry the causally preceding `event_id` when applicable. `trace_id` is propagated from the originating HTTP request (`traceparent` / `X-Trace-Id`) and minted at the edge when absent (never empty). The envelope is mirrored into Kafka headers (`trace_id`, `event_type`) for routing; the JSON envelope remains the source of truth.

---

## Inter-Service Contracts

This service's slice of the project-level `API_CONTRACTS.md` and `EVENT_CONTRACTS.md` (copied verbatim).

### Synchronous dependencies

None — `social_service` has no synchronous internal dependencies (no `API-*` contract is consumed or provided). Peers verify access tokens locally using `identity_service`'s published public keys.

### Events produced

Producer on `social_events` — `EVT-007` (consumer `notification_service`) and `EVT-008` (consumers `messaging_service`, `realtime_gateway`). Registry rows: DEP-010, DEP-011.

### Mandatory event envelope (every event on every topic)

Every message is a JSON object with exactly two top-level keys: `envelope` and `payload`.

| Field | Type | Required | Rule |
|---|---|---|---|
| `event_id` | string (uuid) | yes | Unique per event. Consumer deduplication key. |
| `event_type` | string (snake_case) | yes | Must match a registered contract. |
| `event_version` | integer | yes | >= 1. A breaking payload change bumps it. |
| `occurred_at` | string (RFC3339 UTC) | yes | Producer clock at emit time. |
| `producer` | string (snake_case) | yes | Must equal the registered producer. |
| `trace_id` | string (32 lowercase hex) | yes | **Never empty.** Propagated from the originating request; minted at the edge when absent. |
| `correlation_id` | string (uuid) | yes | Groups every event of one business flow. |
| `causation_id` | string (uuid) or null | yes | `event_id` of the causing event. `null` only for the flow's first event. |
| `schema_ref` | string | yes | `EVENT_CONTRACTS.md#<event_type>-v<version>`. |

Rules:

- The envelope is not optional and not per-topic. A topic needing a field the envelope lacks adds it to `payload`, never to the envelope.
- A payload schema MUST NOT declare an envelope field name at its top level.
- `trace_id` is mandatory even when no HTTP request originated the flow: the producer mints one at the edge of its own flow.
- When a consumer emits an event because of another event, it copies `trace_id` and `correlation_id` from the causing event and sets `causation_id` to the causing `event_id`.
- The envelope is also mirrored into Kafka message headers (`trace_id`, `event_type`) for routing and observability. The JSON envelope remains the source of truth.
- Delivery is at-least-once. Every consumer dedupes on `envelope.event_id` and applies changes idempotently. Projections keep an entity version and ignore stale versions.

Envelope schema (the `envelope` object):

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "additionalProperties": false,
  "required": ["event_id", "event_type", "event_version", "occurred_at", "producer", "trace_id", "correlation_id", "causation_id", "schema_ref"],
  "properties": {
    "event_id": {"type": "string", "format": "uuid"},
    "event_type": {"type": "string", "pattern": "^[a-z][a-z0-9_]*$"},
    "event_version": {"type": "integer", "minimum": 1},
    "occurred_at": {"type": "string", "format": "date-time"},
    "producer": {"type": "string", "pattern": "^[a-z][a-z0-9_]*$"},
    "trace_id": {"type": "string", "pattern": "^[0-9a-f]{32}$"},
    "correlation_id": {"type": "string", "format": "uuid"},
    "causation_id": {"type": ["string", "null"], "format": "uuid"},
    "schema_ref": {"type": "string", "pattern": "^EVENT_CONTRACTS\\.md#[a-z][a-z0-9_]*-v[0-9]+$"}
  }
}
```

### EVT-007 — `friend_request_changed` v1

Emitted when a friend request is sent, accepted, declined, or cancelled.

| Field | Value |
|---|---|
| Producer | `social_service` |
| Consumers | `notification_service` |
| Topic | `social_events` |
| Key | `recipient_user_id` |
| Delivery | at-least-once; consumers dedupe on `envelope.event_id` |

**Payload** (the `payload` object)

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "additionalProperties": false,
  "required": ["request_id", "sender_user_id", "recipient_user_id", "status"],
  "properties": {
    "request_id": {"type": "string", "format": "uuid"},
    "sender_user_id": {"type": "string", "format": "uuid"},
    "recipient_user_id": {"type": "string", "format": "uuid"},
    "status": {"type": "string", "enum": ["pending", "accepted", "declined", "cancelled"]}
  }
}
```

### EVT-008 — `conversation_changed` v1

Emitted when a private conversation (direct or group) is created, its participants change, or it is renamed. The payload always carries the full current participant list, plus a delta.

| Field | Value |
|---|---|
| Producer | `social_service` |
| Consumers | `messaging_service`, `realtime_gateway` |
| Topic | `social_events` |
| Key | `conversation_id` |
| Delivery | at-least-once; consumers dedupe on `envelope.event_id`; apply only if `conversation_version` is newer |

**Payload** (the `payload` object)

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "additionalProperties": false,
  "required": ["conversation_id", "kind", "change", "name", "participant_user_ids", "added_user_ids", "removed_user_ids", "conversation_version"],
  "properties": {
    "conversation_id": {"type": "string", "format": "uuid"},
    "kind": {"type": "string", "enum": ["direct", "group"]},
    "change": {"type": "string", "enum": ["created", "participants_changed", "renamed"]},
    "name": {"type": ["string", "null"], "maxLength": 100},
    "participant_user_ids": {"type": "array", "items": {"type": "string", "format": "uuid"}, "minItems": 1, "maxItems": 10, "uniqueItems": true},
    "added_user_ids": {"type": "array", "items": {"type": "string", "format": "uuid"}, "uniqueItems": true},
    "removed_user_ids": {"type": "array", "items": {"type": "string", "format": "uuid"}, "uniqueItems": true},
    "conversation_version": {"type": "integer", "minimum": 1}
  }
}
```

---

## 5. PostgreSQL database schema

Schema-owned tables (snake_case; all timestamps `timestamptz`; uuid v4 keys; migrations applied at deploy time).

### `friend_request`

One row per friend request; forward-only status machine.

| Column | Type | Constraints |
|---|---|---|
| `id` | uuid | PK |
| `sender_user_id` | uuid | NOT NULL |
| `recipient_user_id` | uuid | NOT NULL |
| `status` | text | NOT NULL CHECK `IN ('pending','accepted','declined','cancelled')` |
| `created_at` | timestamptz | NOT NULL (default now) |
| `updated_at` | timestamptz | NOT NULL |

Constraints & indexes:

- CHECK `sender_user_id <> recipient_user_id` (no self-requests).
- Partial unique index `uq_friend_request_pending_pair` on `(LEAST(sender_user_id, recipient_user_id), GREATEST(sender_user_id, recipient_user_id)) WHERE status = 'pending'` — at most one pending request per pair, in either direction. The friendship-existence check (`409 already_friends`) is applied at write time.
- Index `idx_friend_request_recipient_status` on `(recipient_user_id, status, created_at DESC)` — incoming list.
- Index `idx_friend_request_sender_status` on `(sender_user_id, status, created_at DESC)` — outgoing list.

### `friendship`

The unordered pair, canonically stored with `user_id_a < user_id_b`.

| Column | Type | Constraints |
|---|---|---|
| `user_id_a` | uuid | PK part, CHECK `user_id_a < user_id_b` |
| `user_id_b` | uuid | PK part |
| `created_at` | timestamptz | NOT NULL |

Constraints & indexes:

- PK `(user_id_a, user_id_b)`. The pair is canonicalized on write (`a = LEAST`, `b = GREATEST`), which makes duplicate inserts and mutual-removal races impossible at the storage layer.
- Index `idx_friendship_user_b` on `(user_id_b, created_at DESC)` — reverse lookup for a user's friend list (union with the `user_id_a` side).

### `conversation`

| Column | Type | Constraints |
|---|---|---|
| `id` | uuid | PK |
| `kind` | text | NOT NULL CHECK `IN ('direct','group')` |
| `name` | text | NULL; CHECK `char_length(name) <= 100`; always NULL when `kind = 'direct'` (CHECK `kind <> 'direct' OR name IS NULL`) |
| `created_by` | uuid | NOT NULL |
| `direct_pair_key` | text | NULL; used only for `direct` conversations — canonical `'<lesser uuid>|<greater uuid>'` of the two participants |
| `conversation_version` | integer | NOT NULL, starts at 1, incremented on every mutation |
| `created_at` | timestamptz | NOT NULL |
| `updated_at` | timestamptz | NOT NULL |

Constraints & indexes:

- Partial unique index `uq_direct_pair` on `(direct_pair_key) WHERE kind = 'direct' AND direct_pair_key IS NOT NULL` — at most one direct conversation per pair; the storage guard behind open-or-reuse.
- Index `idx_conversation_created` on `(created_at DESC, id DESC)` — conversation listing cursor.

### `conversation_participant`

| Column | Type | Constraints |
|---|---|---|
| `conversation_id` | uuid | PK part, FK → `conversation(id)` ON DELETE CASCADE |
| `user_id` | uuid | PK part |
| `joined_at` | timestamptz | NOT NULL (default now) |
| `left_at` | timestamptz | NULL; NULL = current participant |

Constraints & indexes:

- PK `(conversation_id, user_id)` — one row per user per conversation.
- Partial index `idx_participant_active` on `(user_id, conversation_id) WHERE left_at IS NULL` — a user's active conversation listing.
- Count check: total current participants ≤ 10 — enforced by the domain service inside the same transaction that inserts participants (the event payload schema also caps at 10).
- The "friend of the caller" rule for additions is enforced by the domain service (join against `friendship`) in the same transaction — all-or-nothing.

### `event_outbox`

Transactional outbox; one row per event to publish.

| Column | Type | Constraints |
|---|---|---|
| `id` | uuid | PK |
| `aggregate_type` | text | NOT NULL CHECK `IN ('friend_request','conversation')` |
| `aggregate_id` | uuid | NOT NULL |
| `event_type` | text | NOT NULL (`friend_request_changed` / `conversation_changed`) |
| `event_version` | integer | NOT NULL |
| `topic` | text | NOT NULL (`social_events`) |
| `partition_key` | text | NOT NULL (recipient/conversation id) |
| `correlation_id` | uuid | NOT NULL |
| `causation_id` | uuid | NULL |
| `trace_id` | text | NOT NULL (32 hex chars) |
| `payload` | jsonb | NOT NULL (full event payload; envelope fields added at publish) |
| `published_at` | timestamptz | NULL — set once produced |
| `created_at` | timestamptz | NOT NULL |

Constraints & indexes:

- Index `idx_outbox_unpublished` on `(created_at) WHERE published_at IS NULL` — publisher loop scan.
- Outbox rows older than a retention window (e.g. 7 days after publish) are purged by a periodic job.

### Relationships

```
conversation 1 ──◀ conversation_participant ∗   (CASCADE on conversation delete)
friend_request ∗      (references user ids only; users live in identity_service — no FK by design)
friendship ∗          (unordered pair of user ids; no FK to a users table — users are external)
```

User ids are **not** foreign-keyed: accounts live in `identity_service` and are outside this database. Integrity of user ids is a client/token-level concern.

---

## 6. Stateless container operational model

- **Statelessness.** No local disk, no in-memory session state, no sticky routing. All state is PostgreSQL; all integration is Kafka. Any replica can serve any request.
- **Probes.**
  - `GET /healthz` (liveness) — process responsiveness only; never fails on PostgreSQL/Kafka hiccups.
  - `GET /readyz` (readiness) — checks the PostgreSQL pool and the Kafka producer connection; returns `503` once shutdown starts.
- **Graceful shutdown.** On `SIGTERM`/`SIGINT`: readiness flips to failing → stop accepting new HTTP requests → finish in-flight requests and drain the outbox publisher loop within a bounded timeout (≤ 30 s) → close the Kafka producer (flushing pending messages) → close the PostgreSQL pool → exit `0`.
- **Failure modes.**

| Failure | Behavior |
|---|---|
| Kafka produce fails / broker down | Outbox rows remain unpublished; publisher retries with exponential back-off (bounded); `/readyz` degrades; HTTP writes still succeed (state + outbox are committed). |
| PostgreSQL unreachable | All writes fail with `500` (`database_unavailable`); readiness fails; no partial state. |
| Crash after commit, before publish | Outbox row survives; the restarted replica's publisher sends it later — at-least-once, consumers dedupe. |
| Concurrent duplicate creates (same pair / same `Idempotency-Key`) | Storage uniqueness guards + idempotency records converge on one result. |
| Duplicate event delivery (producer retry) | Safe: consumers dedupe on `envelope.event_id` and apply version-guarded changes. |

- **Resilience patterns.** Idempotency keys on creation endpoints; transactional outbox; version counters (`conversation_version`) for downstream staleness checks; bounded retries with back-off on publish; no synchronous calls, therefore no circuit breaker needed.
- **Scaling.** `replicas >= 1`; horizontal scaling is safe (Kafka producing is keyed and outbox rows are per-aggregate). At launch scale the load is modest CRUD; single-digit replicas are sufficient.

---

## 7. Observability

### Business metrics

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `social_friend_requests_total` | counter | `action` (`sent|accepted|declined|cancelled`) | Friend-request lifecycle volume |
| `social_friendships_total` | gauge | — | Current number of friendships |
| `social_conversations_created_total` | counter | `kind` (`direct|group`) | Conversation creation (not reuse) |
| `social_conversation_participants_added_total` | counter | — | Participant additions |
| `social_events_published_total` | counter | `event_type` | Outbox publishes |

### Technical (RED) metrics

- `http_requests_total{route,method,status_class}` and `http_request_duration_seconds{route,method}` — standard request metrics; route labels are low-cardinality path templates (never raw ids).
- `kafka_publish_duration_seconds{topic}` and `social_outbox_unpublished` (gauge) — publisher health; a growing outbox gauge alarms before consumers starve.

All labels are low-cardinality (never user, conversation, or request ids).

### Tracing

`traceparent` / `X-Trace-Id` is accepted on every HTTP call, propagated into logs, and travels to consumers inside the mandatory event envelope's `trace_id` (plus mirrored Kafka headers). Flows without an originating request mint a `trace_id` at the edge. Structured JSON logs carry at minimum: timestamp, severity, service name, `trace_id`, operation, status; user ids and message contents never appear in logs.

### SLOs

- Public API p95 < 150 ms; 5xx rate < 0.1 %.
- Event published within 1 s p95 of the state change committing (`social_outbox_unpublished ≈ 0` steady-state).
