package sqlitestore

// migrations[i] moves a database from PRAGMA user_version i to i+1. Open
// applies the missing suffix inside the BEGIN EXCLUSIVE that locks the file,
// so a half-applied migration is never visible and a second process never
// finds the file unlocked. Entries are append-only: editing one that has
// shipped leaves databases already past it on the old shape.
//
// Desired-state columns are written only by CommitDesired and status columns
// only by Commit*Status. They share a row per resource so that "what is
// unconverged?" is one partial-index lookup. Key pairs have no status
// columns: compute.KeyPair.Observed is derived. Metadata columns are written
// only when a change carries metadata, and a spec update does not name them.
var migrations = []string{
	schemaV1,
	metadataV2,
	imageSourcesV3,
}

// schemaV1 is the schema from before user_version was set. Every statement
// is IF NOT EXISTS, so it creates a fresh file and adopts an unversioned
// file without rewriting existing tables.
const schemaV1 = `
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

// metadataV2 adds labels, description, and the metadata revision to every
// resource table, and the MetadataOnly bit to operations. Defaults are the
// empty metadata at revision 0. Labels are canonical JSON.
const metadataV2 = `
ALTER TABLE instances ADD COLUMN labels TEXT NOT NULL DEFAULT '{}';
ALTER TABLE instances ADD COLUMN description TEXT NOT NULL DEFAULT '';
ALTER TABLE instances ADD COLUMN meta_revision INTEGER NOT NULL DEFAULT 0;
ALTER TABLE instances ADD COLUMN meta_changed_at INTEGER NOT NULL DEFAULT 0;
ALTER TABLE images ADD COLUMN labels TEXT NOT NULL DEFAULT '{}';
ALTER TABLE images ADD COLUMN description TEXT NOT NULL DEFAULT '';
ALTER TABLE images ADD COLUMN meta_revision INTEGER NOT NULL DEFAULT 0;
ALTER TABLE images ADD COLUMN meta_changed_at INTEGER NOT NULL DEFAULT 0;
ALTER TABLE key_pairs ADD COLUMN labels TEXT NOT NULL DEFAULT '{}';
ALTER TABLE key_pairs ADD COLUMN description TEXT NOT NULL DEFAULT '';
ALTER TABLE key_pairs ADD COLUMN meta_revision INTEGER NOT NULL DEFAULT 0;
ALTER TABLE key_pairs ADD COLUMN meta_changed_at INTEGER NOT NULL DEFAULT 0;
ALTER TABLE operations ADD COLUMN metadata_only INTEGER NOT NULL DEFAULT 0;
`

// imageSourcesV3 adds file, volume, and snapshot sources. SQLite cannot
// replace the v1 CHECK that allowed only a URL or an instance, so the table
// is rebuilt. No other table references images.
const imageSourcesV3 = `
CREATE TABLE images_v3 (
  id                  TEXT PRIMARY KEY,
  generation          INTEGER NOT NULL CHECK (generation >= 1),
  created_at          INTEGER NOT NULL,
  changed_at          INTEGER NOT NULL,
  name                TEXT NOT NULL,
  source_url          TEXT,
  source_instance_id  TEXT,
  source_path         TEXT,
  source_volume       TEXT,
  source_snapshot     TEXT,
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
  labels              TEXT NOT NULL DEFAULT '{}',
  description         TEXT NOT NULL DEFAULT '',
  meta_revision       INTEGER NOT NULL DEFAULT 0,
  meta_changed_at     INTEGER NOT NULL DEFAULT 0,
  CHECK ((source_url IS NOT NULL) + (source_instance_id IS NOT NULL) + (source_path IS NOT NULL)
       + (source_volume IS NOT NULL) + (source_snapshot IS NOT NULL) = 1),
  CHECK (observed_generation <= generation)
) STRICT;

INSERT INTO images_v3 (
  id, generation, created_at, changed_at, name, source_url, source_instance_id,
  source_path, source_volume, source_snapshot, deregistered, observed_generation,
  failure_code, failure_message, present, size_bytes, attempts, retry_at, last_error,
  updated_at, labels, description, meta_revision, meta_changed_at)
SELECT
  id, generation, created_at, changed_at, name, source_url, source_instance_id,
  NULL, NULL, NULL, deregistered, observed_generation,
  failure_code, failure_message, present, size_bytes, attempts, retry_at, last_error,
  updated_at, labels, description, meta_revision, meta_changed_at
FROM images;

DROP TABLE images;
ALTER TABLE images_v3 RENAME TO images;
CREATE INDEX images_unconverged ON images (id) WHERE generation > observed_generation;
`
