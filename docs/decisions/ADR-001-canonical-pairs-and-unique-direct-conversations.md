# ADR-001 — Canonical unordered friendship pairs and unique direct conversations

**Status:** Accepted · **Date:** 2026-07-30 · **Decided by:** Tech Lead (tech-lead)

## Context

`social_service` stores two inherently symmetric relationships:

1. **Friendship** — an unordered pair of users. Storing it directionally
   (`sender → recipient`) would double every read (check both directions on
   every friend lookup), duplicate removals, and allow contradictory rows.
2. **Direct conversation** — the platform requires exactly one 1:1
   conversation per pair, opened idempotently ("open or reuse"). Two
   concurrent "open DM" requests must converge on one conversation.

## Decision

- **`friendship` rows are canonicalized on write:** `user_id_a = LEAST(a, b)`,
  `user_id_b = GREATEST(a, b)`, enforced by a CHECK `user_id_a < user_id_b`,
  with PK `(user_id_a, user_id_b)`. Friendship removal deletes the single
  canonical row; mutual removal races are no-ops rather than errors.
- **Direct conversations carry a `direct_pair_key`** (the same
  `'<lesser uuid>|<greater uuid>'` string) with a partial unique index
  `WHERE kind = 'direct' AND direct_pair_key IS NOT NULL`. "Open or reuse"
  relies on INSERT … ON CONFLICT semantics inside the same transaction, so
  the concurrent loser returns the existing conversation with `200`.
- **One pending friend request per pair, either direction**, enforced by a
  partial unique index on the canonical pair `WHERE status = 'pending'`.

## Consequences

- All friendship/direct-pair checks are single-row lookups (no OR over two
  orderings) — simpler and faster.
- The domain layer computes the canonical key; storage guarantees uniqueness.
- Group conversations intentionally have no equivalent constraint (multiple
  groups with overlapping members are allowed).

## Alternatives considered

- Directional friendship rows with symmetric queries — rejected: duplicated
  logic, no storage-level integrity, race-prone removal.
- Enforcing one-direct-conversation only in application logic — rejected:
  a uniqueness guard is the only race-free option under concurrent requests.
