# User Stories — `social_service`

Friend requests, friendships, and private conversations (direct / group) for the
Community Chat & Voice Platform.

Roles used below:

- **Member** — an authenticated end user (verified access token). Every story's actor is a Member unless stated otherwise.
- Friend requests are addressed by `user_id`; the client resolves usernames to ids via `identity_service`'s public lookup before calling this service. `social_service` itself never calls another service at request time.

---

## US-001: Send a friend request

**As a** Member,
**I want to** send a friend request to another user by their user id,
**So that** they can accept me into their friend list and we can start talking privately.

### Acceptance Criteria
- Given I am authenticated, when I send a request with a valid `recipient_user_id` that is not myself, then the request is created with status `pending` and I receive the request representation (id, sender, recipient, status, created_at).
- Given the recipient already has a pending request **from me**, when I send again, then I receive `409 request_pending` and no duplicate request is created.
- Given a pending request exists **from them to me**, when I send them a request, then I receive `409 request_pending` (one pending request per pair, in either direction).
- Given we are already friends, when I send a request, then I receive `409 already_friends`.
- Given I target myself as recipient, when I send, then I receive `422 self_request` and nothing is stored.
- Given an identical send is retried after a network failure (same `Idempotency-Key`), when the request arrives again, then the original pending request is returned and no duplicate is created.
- Given the request is created, then a `friend_request_changed` event with status `pending` is published so the recipient can be notified.

### Out of Scope
- Discovering users to befriend (client-side, from `identity_service` lookup and shared community member lists).
- Blocking/reporting users.
- Verifying that the recipient id exists (no synchronous identity call; an unknown id simply never accepts).

---

## US-002: Review incoming and outgoing friend requests

**As a** Member,
**I want to** list the friend requests addressed to me and the ones I sent,
**So that** I can decide who to accept and see whose requests are still open.

### Acceptance Criteria
- Given I have pending requests, when I list with `direction=incoming`, then only pending requests where I am the recipient are returned, newest first.
- Given I have pending requests, when I list with `direction=outgoing`, then only pending requests where I am the sender are returned, newest first.
- Given `direction` is omitted, when I list, then `incoming` is used as the default.
- Given the list is longer than `limit`, when I page with the returned cursor, then the next page is returned without duplication; `next_cursor` is `null` on the last page.
- Given a listed request was cancelled or accepted between pages, when I page, then stale entries are either absent or carry their current status, never duplicated.

### Out of Scope
- Showing the requester's profile fields (display name/avatar) — the client hydrates user ids through `identity_service`.

---

## US-003: Accept a friend request

**As a** Member,
**I want to** accept a pending incoming friend request,
**So that** we become friends and can open private conversations.

### Acceptance Criteria
- Given a pending incoming request addressed to me, when I accept it, then the request becomes `accepted`, a friendship is created between the two users, and I receive the friendship representation.
- Given the request is already accepted, when I accept again, then I receive the same friendship and no duplicate friendship is created (idempotent).
- Given a request I only sent (not received), when I try to accept it, then I receive `404` and nothing changes.
- Given a request that is `declined` or `cancelled`, when I accept, then I receive `409 request_not_pending`.
- Given the accept succeeds, then a `friend_request_changed` event with status `accepted` is published so the sender can be notified.

### Out of Scope
- Notification delivery (`notification_service` consumes the event).

---

## US-004: Decline a friend request

**As a** Member,
**I want to** decline a pending incoming friend request,
**So that** I can refuse a connection without confronting the sender.

### Acceptance Criteria
- Given a pending incoming request addressed to me, when I decline it, then the request becomes `declined` and I receive `204 No Content`.
- Given a request I did not receive, when I decline it, then I receive `404`.
- Given a request already `declined` or `cancelled`, when I decline again, then I receive `409 request_not_pending`.
- Given the decline succeeds, then a `friend_request_changed` event with status `declined` is published.

### Out of Scope
- Any notification to the sender about the decline (events are consumed downstream; the sender simply never sees an acceptance).
- Re-sending the same request (a new request can be created later; no sender-side "cooldown" at launch).

---

## US-005: Cancel an outgoing friend request

**As a** Member,
**I want to** cancel a friend request I sent that is still pending,
**So that** I can change my mind before the other person responds.

### Acceptance Criteria
- Given a pending request I sent, when I delete it, then the request becomes `cancelled` and I receive `204 No Content`.
- Given a pending request addressed to me, when I try to cancel it, then I receive `404` (only the sender cancels).
- Given a request that is already accepted, when I cancel it, then I receive `409 request_not_pending` (unfriend is the correct action instead).

### Out of Scope
- Removing an existing friendship (covered by US-007).

---

## US-006: See my friends

**As a** Member,
**I want to** list my friends,
**So that** I can open direct conversations and see my social graph.

### Acceptance Criteria
- Given I have friends, when I list them, then each entry carries the friend's `user_id` and the date we became friends, ordered by friendship creation descending.
- Given I have no friends yet, when I list them, then an empty list is returned — the client uses this empty state to suggest finding people from shared community member lists (client-side).
- Given the list is longer than `limit`, when I page with the cursor, then pages are stable and complete.

### Out of Scope
- Profile rendering (identity lookup is client-side).
- Friend suggestions (client-side, out of this service).

---

## US-007: Remove a friend

**As a** Member,
**I want to** remove someone from my friend list,
**So that** I can end a connection I no longer want.

### Acceptance Criteria
- Given we are friends, when I remove them, then the friendship is deleted for both sides and I receive `204 No Content`.
- Given we are not friends, when I remove them, then I receive `404 not_friends`.
- Given there is a pending request between us after removal, when I check my requests, then any pre-existing request state is independent of the removed friendship (removal does not auto-create or auto-cancel requests).
- Given either side removes the friendship, when the other side lists friends, then the removed friend no longer appears (friendship is one shared record, removal is mutual and immediate).

### Out of Scope
- Shared group conversations are unaffected by unfriending (participants keep their membership).

---

## US-008: Open a direct conversation

**As a** Member,
**I want to** open a 1:1 direct conversation with one of my friends,
**So that** we can chat privately outside any community.

### Acceptance Criteria
- Given we are friends and have no direct conversation yet, when I open a direct conversation, then a conversation of kind `direct` is created with both of us as participants and I receive `201` with the conversation representation.
- Given a direct conversation already exists between us, when I open it again, then I receive `200` with the **existing** conversation — a second conversation is never created (idempotent per pair).
- Given we are not friends, when I open a direct conversation, then I receive `422 not_friends` and nothing is created.
- Given I target myself, when I open a direct conversation, then I receive `422 self_request` and nothing is created.
- Given the conversation is created, then a `conversation_changed` event (`created`) is published so `messaging_service` learns the participant set and `realtime_gateway` learns the new scope.
- Given the conversation already existed, when the request is repeated, then **no** new event is published.

### Out of Scope
- Sending messages (owned by `messaging_service`, authorized from this service's participant data).

---

## US-009: Create a group conversation

**As a** Member,
**I want to** start a small group conversation with me plus up to 9 of my friends,
**So that** a small circle of friends can chat together.

### Acceptance Criteria
- Given I pass 1–9 friend user ids, when I create the group, then a conversation of kind `group` is created with me (creator) and all listed friends as participants, and I receive `201` with the conversation.
- Given I pass an optional name (≤ 100 chars), when I create the group, then the name is stored and returned.
- Given I pass more than 9 friend ids, when I create the group, then I receive `422 group_too_large` and nothing is created.
- Given any listed user is not my friend, when I create the group, then I receive `422 not_friends` and nothing is created (all-or-nothing).
- Given I pass zero participants, when I create the group, then I receive `422 missing_participants`.
- Given I list the same friend twice, when I create the group, then the duplicate is ignored and the participant list is unique.
- Given the group is created, then a `conversation_changed` event (`created`) is published.

### Out of Scope
- Communities, channels, and voice (owned by other services); group size beyond 10 total participants.

---

## US-010: See my conversations

**As a** Member,
**I want to** list the direct and group conversations I currently participate in,
**So that** the DM sidebar can show what I can open.

### Acceptance Criteria
- Given I participate in conversations, when I list them, then each entry carries kind, name (nullable for direct), participants, creator, and creation time, ordered by creation descending.
- Given I left a group conversation, when I list, then that conversation is **not** returned (left participation is excluded).
- Given I have no conversations, when I list, then an empty list is returned.
- Given the list is longer than `limit`, when I page with the cursor, then pages are stable and complete.

### Out of Scope
- Unread counts and last-message previews (owned by `messaging_service`).

---

## US-011: Add friends to a group conversation

**As a** current participant of a group conversation,
**I want to** add more of my friends to it,
**So that** the group can grow without creating a new chat.

### Acceptance Criteria
- Given I am a current participant, when I add friend user ids, then they become participants and I receive the updated conversation; total participants never exceed 10.
- Given the addition would exceed 10 total participants, when I submit it, then I receive `422 group_full` and nobody is added (all-or-nothing).
- Given any listed user is not my friend, when I submit, then I receive `422 not_friends` and nobody is added.
- Given I am not a current participant, when I try to add participants, then I receive `403 not_a_participant`.
- Given a user already in the group, when I add them again, then the duplicate is ignored and the participant set is unchanged for that user.
- Given the addition succeeds, then a `conversation_changed` event (`participants_changed`) is published with the full participant list plus `added_user_ids`.

### Out of Scope
- Removing other participants (no removal endpoint at launch; participants may leave themselves).

---

## US-012: Leave a group conversation

**As a** current participant of a group conversation,
**I want to** leave it,
**So that** I stop receiving its messages and it disappears from my sidebar.

### Acceptance Criteria
- Given I am a current participant, when I leave, then my participation is ended (I disappear from the participant list) and I receive `204 No Content`.
- Given I am not a participant (or already left), when I leave, then I receive `404`.
- Given the conversation is `direct`, when I try to leave, then I receive `422 group_only_operation` (direct conversations are not leaveable at launch).
- Given the leave succeeds, then a `conversation_changed` event (`participants_changed`) is published with the updated list and `removed_user_ids = [me]`.

### Out of Scope
- Deleting the conversation; the last participant leaving leaves an empty conversation rather than deleting it.

---

## US-013: Rename a group conversation

**As a** current participant of a group conversation,
**I want to** rename it,
**So that** the group's purpose stays clear to everyone.

### Acceptance Criteria
- Given I am a current participant, when I rename the group with a name of 1–100 characters, then the name is updated and I receive the updated conversation.
- Given I am not a current participant, when I try to rename, then I receive `403 not_a_participant`.
- Given the conversation is `direct`, when I try to rename, then I receive `422 group_only_operation` (direct conversations are nameless by definition).
- Given an empty or oversized name, when I submit, then I receive `422 invalid_name`.
- Given the rename succeeds, then a `conversation_changed` event (`renamed`) is published.

### Out of Scope
- Icons/avatars for conversations.

---

## Cross-cutting stories

## US-101: Authentication on every endpoint

**As a** Platform operator,
**I want to** every `social_service` endpoint to require a valid, verified access token,
**So that** only real account holders can read or mutate social state.

### Acceptance Criteria
- Given a request without an `Authorization: Bearer` token, when it reaches any endpoint other than `/healthz` and `/readyz`, then it is rejected with `401 unauthorized`.
- Given a token that is expired, malformed, or signed by an untrusted key, when it is presented, then it is rejected with `401 unauthorized` (verification is local, using `identity_service`'s published public keys).
- Given a token whose `sub` refers to a user id, when any request is processed, then every read and write is scoped to that user id — a caller can never see or mutate another user's requests, friendships, or conversations.
- Given a valid token with `email_verified: false`, when it is presented, then the request is processed normally (email verification gating is `identity_service`'s concern).

### Out of Scope
- Refresh-token handling and sign-in (owned by `identity_service`).

## US-102: Downstream systems learn about social changes

**As a** Platform operator,
**I want to** every friend-request and conversation change published as an event on `social_events`,
**So that** `notification_service` can notify users and `messaging_service` / `realtime_gateway` can enforce access and push live updates.

### Acceptance Criteria
- Given any friend-request state change, when it is committed, then exactly one `friend_request_changed` event with a full status snapshot is published on `social_events` (key: `recipient_user_id`).
- Given any conversation creation, participant change, or rename, when it is committed, then exactly one `conversation_changed` event with the full current participant list is published on `social_events` (key: `conversation_id`).
- Given a retry after a broker failure, when the producer resends, then duplicate events are tolerated downstream (at-least-once; consumers dedupe on `envelope.event_id`).
- Given every published event, when inspected, then it carries the mandatory envelope with a non-empty `trace_id` propagated from the originating HTTP request (or minted at the edge).

### Out of Scope
- Consuming any event (this service produces only).
