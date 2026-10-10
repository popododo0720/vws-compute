package sqlitestore

// schema is applied at every Open. Desired-state columns are written only by
// CommitDesired and status columns only by Commit*Status. They share a row
// per resource so that "what is unconverged?" is one partial-index lookup.
// Key pairs have no status columns: compute.KeyPair.Observed is derived.
const schema = `
CREATE TABLE IF NOT EXISTS instances (
  id                  TEXT PRIMARY KEY,
  generation          INTEGER NOT NULL CHECK (generation >= 1),
  created_at          INTEGER NOT NULL,
  changed_at          INTEGER NOT NULL,
  image_id            TEXT NOT NULL,
  instance_type       TEXT NOT NULL,
  key_name            TEXT,
  key_public          TEXT,
  power               INTEGER NOT NULL,
  reboot_epoch        INTEGER NOT NULL,
  observed_generation INTEGER NOT NULL DEFAULT 0,
  failure_code        INTEGER,
  failure_message     TEXT,
  machine             INTEGER NOT NULL DEFAULT 0,
  applied_epoch       INTEGER NOT NULL DEFAULT 0,
  attempts            INTEGER NOT NULL DEFAULT 0,
  retry_at            INTEGER NOT NULL DEFAULT 0,
  last_error          TEXT NOT NULL DEFAULT '',
  updated_at          INTEGER NOT NULL DEFAULT 0,
  CHECK (observed_generation <= generation)
) STRICT;

CREATE INDEX IF NOT EXISTS instances_unconverged ON instances (id)
  WHERE generation > observed_generation;

CREATE TABLE IF NOT EXISTS images (
  id                  TEXT PRIMARY KEY,
  generation          INTEGER NOT NULL CHECK (generation >= 1),
  created_at          INTEGER NOT NULL,
  changed_at          INTEGER NOT NULL,
  name                TEXT NOT NULL,
  source_url          TEXT,
  source_instance_id  TEXT,
  deregistered        INTEGER NOT NULL,
  observed_generation INTEGER NOT NULL DEFAULT 0,
  failure_code        INTEGER,
  failure_message     TEXT,
  present             INTEGER NOT NULL DEFAULT 0,
  size_bytes          INTEGER NOT NULL DEFAULT 0,
  attempts            INTEGER NOT NULL DEFAULT 0,
  retry_at            INTEGER NOT NULL DEFAULT 0,
  last_error          TEXT NOT NULL DEFAULT '',
  updated_at          INTEGER NOT NULL DEFAULT 0,
  CHECK ((source_url IS NULL) <> (source_instance_id IS NULL)),
  CHECK (observed_generation <= generation)
) STRICT;

CREATE INDEX IF NOT EXISTS images_unconverged ON images (id)
  WHERE generation > observed_generation;

CREATE TABLE IF NOT EXISTS key_pairs (
  id          TEXT PRIMARY KEY,
  generation  INTEGER NOT NULL CHECK (generation >= 1),
  created_at  INTEGER NOT NULL,
  changed_at  INTEGER NOT NULL,
  name        TEXT NOT NULL,
  public_key  TEXT NOT NULL,
  deleted     INTEGER NOT NULL
) STRICT;

CREATE UNIQUE INDEX IF NOT EXISTS key_pairs_live_name ON key_pairs (name) WHERE deleted = 0;

CREATE TABLE IF NOT EXISTS operations (
  id                TEXT PRIMARY KEY,
  request_id        TEXT NOT NULL UNIQUE,
  fingerprint       BLOB NOT NULL,
  kind              TEXT NOT NULL,
  target_kind       INTEGER NOT NULL,
  target_id         TEXT NOT NULL,
  target_generation INTEGER NOT NULL,
  state             INTEGER NOT NULL,
  failure_code      INTEGER,
  failure_message   TEXT,
  created_at        INTEGER NOT NULL,
  settled_at        INTEGER NOT NULL DEFAULT 0
) STRICT;

CREATE INDEX IF NOT EXISTS operations_pending_by_target ON operations (target_kind, target_id)
  WHERE state = 1;
`
