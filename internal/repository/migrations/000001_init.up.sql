-- Schema for social_service (architecture.md §5).
-- User ids are deliberately NOT foreign-keyed: accounts live in identity_service.

CREATE TABLE friend_request (
    id uuid PRIMARY KEY,
    sender_user_id uuid NOT NULL,
    recipient_user_id uuid NOT NULL,
    status text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT friend_request_no_self CHECK (sender_user_id <> recipient_user_id),
    CONSTRAINT friend_request_status_check CHECK (status IN ('pending', 'accepted', 'declined', 'cancelled'))
);

-- At most one pending request per pair, in either direction.
CREATE UNIQUE INDEX uq_friend_request_pending_pair
    ON friend_request (LEAST(sender_user_id, recipient_user_id), GREATEST(sender_user_id, recipient_user_id))
    WHERE status = 'pending';

CREATE INDEX idx_friend_request_recipient_status
    ON friend_request (recipient_user_id, status, created_at DESC);

CREATE INDEX idx_friend_request_sender_status
    ON friend_request (sender_user_id, status, created_at DESC);

CREATE TABLE friendship (
    user_id_a uuid NOT NULL,
    user_id_b uuid NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (user_id_a, user_id_b),
    CONSTRAINT friendship_canonical_order CHECK (user_id_a < user_id_b)
);

CREATE INDEX idx_friendship_user_b ON friendship (user_id_b, created_at DESC);

CREATE TABLE conversation (
    id uuid PRIMARY KEY,
    kind text NOT NULL,
    name text,
    created_by uuid NOT NULL,
    direct_pair_key text,
    conversation_version integer NOT NULL DEFAULT 1,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT conversation_kind_check CHECK (kind IN ('direct', 'group')),
    CONSTRAINT conversation_name_length CHECK (name IS NULL OR char_length(name) BETWEEN 1 AND 100),
    CONSTRAINT conversation_direct_name_null CHECK (kind <> 'direct' OR name IS NULL),
    CONSTRAINT conversation_version_positive CHECK (conversation_version >= 1)
);

-- At most one direct conversation per canonical pair: the storage guard behind
-- open-or-reuse (ADR-001).
CREATE UNIQUE INDEX uq_direct_pair
    ON conversation (direct_pair_key)
    WHERE kind = 'direct' AND direct_pair_key IS NOT NULL;

CREATE INDEX idx_conversation_created ON conversation (created_at DESC, id DESC);

CREATE TABLE conversation_participant (
    conversation_id uuid NOT NULL REFERENCES conversation (id) ON DELETE CASCADE,
    user_id uuid NOT NULL,
    joined_at timestamptz NOT NULL DEFAULT now(),
    left_at timestamptz,
    PRIMARY KEY (conversation_id, user_id)
);

CREATE INDEX idx_participant_active
    ON conversation_participant (user_id, conversation_id)
    WHERE left_at IS NULL;

CREATE TABLE event_outbox (
    id uuid PRIMARY KEY,
    aggregate_type text NOT NULL,
    aggregate_id uuid NOT NULL,
    event_type text NOT NULL,
    event_version integer NOT NULL,
    topic text NOT NULL,
    partition_key text NOT NULL,
    correlation_id uuid NOT NULL,
    causation_id uuid,
    trace_id text NOT NULL,
    payload jsonb NOT NULL,
    published_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT event_outbox_aggregate_type_check CHECK (aggregate_type IN ('friend_request', 'conversation')),
    CONSTRAINT event_outbox_event_version_check CHECK (event_version >= 1),
    CONSTRAINT event_outbox_trace_id_check CHECK (trace_id ~ '^[0-9a-f]{32}$')
);

CREATE INDEX idx_outbox_unpublished ON event_outbox (created_at) WHERE published_at IS NULL;

-- Idempotency records for the creation endpoints. The outcome is stored as a
-- reference to the created row (plus the status code) so a replay returns the
-- original result without duplicating state.
CREATE TABLE idempotency_record (
    user_id uuid NOT NULL,
    endpoint text NOT NULL,
    idempotency_key text NOT NULL,
    request_hash text NOT NULL,
    status_code integer NOT NULL,
    resource_id uuid NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (user_id, endpoint, idempotency_key)
);

CREATE INDEX idx_idempotency_created ON idempotency_record (created_at);
