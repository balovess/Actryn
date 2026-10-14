-- Development bootstrap; production should use an explicit migration runner.
CREATE TABLE IF NOT EXISTS shard_leases (
    shard_id integer PRIMARY KEY CHECK (shard_id >= 0 AND shard_id < 256),
    owner_id text NOT NULL,
    endpoint text NOT NULL,
    epoch bigint NOT NULL CHECK (epoch > 0),
    expires_at timestamptz NOT NULL
);

CREATE TABLE IF NOT EXISTS actor_states (
    kind text NOT NULL,
    actor_key text NOT NULL,
    version bigint NOT NULL CHECK (version > 0),
    payload bytea NOT NULL,
    PRIMARY KEY (kind,actor_key)
);

CREATE TABLE IF NOT EXISTS actor_receipts (
    kind text NOT NULL,
    actor_key text NOT NULL,
    request_id text NOT NULL,
    request_hash bytea NOT NULL CHECK (octet_length(request_hash)=32),
    reply bytea NOT NULL,
    PRIMARY KEY(kind,actor_key,request_id)
);

CREATE TABLE IF NOT EXISTS actor_outbox (
    outbox_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    kind text NOT NULL,
    actor_key text NOT NULL,
    request_id text NOT NULL,
    ordinal integer NOT NULL,
    destination_kind text NOT NULL,
    destination_key text NOT NULL,
    payload bytea NOT NULL,
    delivered boolean NOT NULL DEFAULT false,
    UNIQUE(kind,actor_key,request_id,ordinal)
);

CREATE INDEX IF NOT EXISTS actor_outbox_pending_idx
ON actor_outbox (outbox_id) WHERE NOT delivered;
