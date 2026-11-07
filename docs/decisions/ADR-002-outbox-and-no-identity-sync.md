# ADR-002 — Transactional outbox and no synchronous identity dependency

**Status:** Accepted · **Date:** 2026-07-30 · **Decided by:** Tech Lead (tech-lead)

## Context

`social_service` is the authority on conversation participants; `messaging_service`
and `realtime_gateway` enforce access and live-push from EVT-008, and
`notification_service` notifies from EVT-007. Two risks had to be designed out:

1. A state change committed without its event (or vice versa) leaves
   `messaging_service`'s projection and the real-time scopes inconsistent
   with the participant authority — a user could post to a conversation they
   left, or be denied one they joined.
2. A synchronous call to `identity_service` to "validate" recipient or
   participant user ids would introduce the platform's first outbound
   dependency for a purely local write, adding latency and a failure mode
   the rest of the design does not need.

## Decision

1. **Transactional outbox.** Every state change commits its `event_outbox`
   row in the same PostgreSQL transaction; a publisher loop produces to
   Kafka and marks rows published. Delivery is at-least-once; consumers
   dedupe on `envelope.event_id` and apply `conversation_version`-guarded
   changes, so producer retries and crash-after-commit are harmless.
2. **No user-existence verification.** Recipient/participant ids are trusted
   from the verified access-token flow; the client resolves usernames to ids
   through `identity_service`'s public lookup. An unknown id simply never
   accepts and never sees the conversation. This keeps `social_service`
   dependency-free at request time (matching the Dependency Registry: no
   `API-*` edge touches this service).

## Consequences

- Event emission is atomic with state; `social_outbox_unpublished` gauges
  publisher health and alerts before consumers starve.
- A fabricated or typo'd user id costs nothing (an inert row); no spam-able
  existence oracle is exposed.
- If the product later demands eager validation of ids, it requires a new
  ADR and a synchronous dependency — not a silent call.

## Alternatives considered

- Publish directly on the Kafka client after commit — rejected: loses the
  atomicity guarantee and can drop events on broker failure.
- Call `identity_service /users?ids=` before writes — rejected: adds a
  runtime dependency and latency the platform explicitly avoids for this
  service; contradicts the "no synchronous dependencies" design.
