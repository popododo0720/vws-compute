// Package sqlitestore is the SQLite Repository, built on the pure-Go
// modernc.org/sqlite driver.
//
// It is the only package that imports database/sql or a SQLite driver. Rows
// and SQL are unexported. One process owns the database file: Open uses one
// connection in exclusive locking mode, so a second process pointed at the
// same file fails instead of splitting control-plane truth.
package sqlitestore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	_ "modernc.org/sqlite"

	"github.com/popododo0720/vws-compute/internal/compute"
	"github.com/popododo0720/vws-compute/internal/store"
)

var _ store.Repository = (*Store)(nil)

// Store wraps one *sql.DB limited to one connection.
type Store struct {
	db *sql.DB
}

// Open opens or creates the database at path and applies the schema.
func Open(ctx context.Context, path string) (*Store, error) {
	dsn := "file:" + path +
		"?_pragma=journal_mode(WAL)&_pragma=synchronous(FULL)&_pragma=locking_mode(EXCLUSIVE)&_pragma=busy_timeout(0)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	if err := migrate(ctx, db); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("sqlitestore: open %s: %w", path, err)
	}
	return &Store{db: db}, nil
}

// migrate takes the file's write lock and brings the schema up to date in
// one transaction. locking_mode=EXCLUSIVE keeps the lock after COMMIT until
// Close, so a second Open fails whether or not this one migrated anything.
// A file whose user_version is ahead of this binary is refused.
func migrate(ctx context.Context, db *sql.DB) error {
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, `BEGIN EXCLUSIVE`); err != nil {
		return fmt.Errorf("lock: %w", err)
	}
	rollback := func(err error) error {
		_, _ = conn.ExecContext(ctx, `ROLLBACK`)
		return err
	}
	var v int
	if err := conn.QueryRowContext(ctx, `PRAGMA user_version`).Scan(&v); err != nil {
		return rollback(err)
	}
	if v > len(migrations) {
		return rollback(fmt.Errorf("schema version %d is newer than this binary's %d", v, len(migrations)))
	}
	for i := v; i < len(migrations); i++ {
		if _, err := conn.ExecContext(ctx, migrations[i]); err != nil {
			return rollback(fmt.Errorf("migration %d: %w", i+1, err))
		}
	}
	if v < len(migrations) {
		if _, err := conn.ExecContext(ctx, fmt.Sprintf(`PRAGMA user_version = %d`, len(migrations))); err != nil {
			return rollback(err)
		}
	}
	if _, err := conn.ExecContext(ctx, `COMMIT`); err != nil {
		return err
	}
	return nil
}

func (s *Store) Close() error { return s.db.Close() }

type querier interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

func notFound(err error, format string, args ...any) error {
	if errors.Is(err, sql.ErrNoRows) {
		return compute.Errorf(compute.CodeNotFound, format, args...)
	}
	return err
}

func (s *Store) Instance(ctx context.Context, id compute.InstanceID) (compute.Instance, error) {
	inst, err := scanInstance(s.db.QueryRowContext(ctx, `SELECT `+instanceColumns+` FROM instances WHERE id = ?`, id.String()))
	return inst, notFound(err, "instance %s not found", id)
}

func (s *Store) Instances(ctx context.Context, q store.InstanceQuery) (store.Page[compute.Instance], error) {
	only := make([]string, 0, len(q.IDs))
	for _, id := range q.IDs {
		only = append(only, id.String())
	}
	return list(ctx, s.db, "instances", instanceColumns, "id", "", only, q.After, q.PageSize, scanInstance,
		func(i compute.Instance) string { return i.Spec.ID.String() })
}

func (s *Store) Image(ctx context.Context, id compute.ImageID) (compute.Image, error) {
	img, err := scanImage(s.db.QueryRowContext(ctx, `SELECT `+imageColumns+` FROM images WHERE id = ?`, id.String()))
	return img, notFound(err, "image %s not found", id)
}

func (s *Store) Images(ctx context.Context, q store.ImageQuery) (store.Page[compute.Image], error) {
	only := make([]string, 0, len(q.IDs))
	for _, id := range q.IDs {
		only = append(only, id.String())
	}
	return list(ctx, s.db, "images", imageColumns, "id", "", only, q.After, q.PageSize, scanImage,
		func(i compute.Image) string { return i.Spec.ID.String() })
}

func (s *Store) KeyPair(ctx context.Context, id compute.KeyPairID) (compute.KeyPair, error) {
	kp, err := scanKeyPair(s.db.QueryRowContext(ctx, `SELECT `+keyPairColumns+` FROM key_pairs WHERE id = ?`, id.String()))
	return kp, notFound(err, "key pair %s not found", id)
}

func (s *Store) KeyPairByName(ctx context.Context, name compute.KeyName) (compute.KeyPair, error) {
	kp, err := scanKeyPair(s.db.QueryRowContext(ctx, `SELECT `+keyPairColumns+` FROM key_pairs WHERE name = ?
		ORDER BY deleted ASC, changed_at DESC, id DESC LIMIT 1`, name.String()))
	return kp, notFound(err, "key pair %q not found", name)
}

func (s *Store) KeyPairs(ctx context.Context, q store.KeyPairQuery) (store.Page[compute.KeyPair], error) {
	only := make([]string, 0, len(q.Names))
	for _, n := range q.Names {
		only = append(only, n.String())
	}
	return list(ctx, s.db, "key_pairs", keyPairColumns, "name", "deleted = 0", only, q.After, q.PageSize, scanKeyPair,
		func(k compute.KeyPair) string { return k.Spec.Name.String() })
}

// list pages through table ordered by key. It fetches one extra row to know
// whether another page exists.
func list[T any](ctx context.Context, q querier, table, columns, key, where string, only []string, after string,
	size int, scan func(scanner) (T, error), keyOf func(T) string,
) (store.Page[T], error) {
	limit := store.PageLimit(size)
	query := `SELECT ` + columns + ` FROM ` + table + ` WHERE ` + key + ` > ?`
	args := []any{after}
	if where != "" {
		query += ` AND ` + where
	}
	if len(only) > 0 {
		query += ` AND ` + key + ` IN (?` + strings.Repeat(`, ?`, len(only)-1) + `)`
		for _, v := range only {
			args = append(args, v)
		}
	}
	query += ` ORDER BY ` + key + ` LIMIT ?`
	args = append(args, limit+1)
	items, err := collect(ctx, q, scan, query, args...)
	if err != nil {
		return store.Page[T]{}, err
	}
	var p store.Page[T]
	if len(items) > limit {
		items = items[:limit]
		p.Next = keyOf(items[limit-1])
	}
	p.Items = items
	return p, nil
}

func collect[T any](ctx context.Context, q querier, scan func(scanner) (T, error), query string, args ...any) ([]T, error) {
	rows, err := q.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []T{}
	for rows.Next() {
		v, err := scan(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, v)
	}
	return items, rows.Err()
}

func (s *Store) Operation(ctx context.Context, id compute.OperationID) (compute.Operation, error) {
	op, err := scanOperation(s.db.QueryRowContext(ctx, `SELECT `+operationColumns+` FROM operations WHERE id = ?`, id.String()))
	return op, notFound(err, "operation %s not found", id)
}

func (s *Store) OperationByRequest(ctx context.Context, req compute.RequestID) (compute.Operation, error) {
	return operationByRequest(ctx, s.db, req)
}

func operationByRequest(ctx context.Context, q querier, req compute.RequestID) (compute.Operation, error) {
	op, err := scanOperation(q.QueryRowContext(ctx, `SELECT `+operationColumns+` FROM operations WHERE request_id = ?`, req.String()))
	return op, notFound(err, "no operation for request %s", req)
}

func (s *Store) CommitDesired(ctx context.Context, c store.DesiredChange) (compute.Operation, error) {
	var op compute.Operation
	err := s.inTx(ctx, func(tx *sql.Tx) error {
		existing, err := operationByRequest(ctx, tx, c.Op.Request)
		switch {
		case err == nil:
			if existing.Fingerprint != c.Op.Fingerprint {
				return compute.Errorf(compute.CodeRequestMismatch, "request id %s was used for a different request", c.Op.Request)
			}
			op = existing
			return nil
		case compute.CodeOf(err) != compute.CodeNotFound:
			return err
		}
		for _, v := range c.Expect {
			gen, obs, err := progress(ctx, tx, v.Ref)
			if err != nil {
				return err
			}
			if !v.Matches(gen, obs) {
				return store.ErrConflict
			}
		}
		ref := c.Spec.Ref()
		if ref != c.Op.Target || c.Spec.Gen() != c.Op.TargetGeneration {
			return compute.Errorf(compute.CodeInternal, "operation %s does not target the change", c.Op.ID)
		}
		cur, _, err := progress(ctx, tx, ref)
		if err != nil {
			return err
		}
		if c.Write {
			if c.Spec.Gen() != cur+1 {
				return store.ErrConflict
			}
		} else if c.Spec.Gen() != cur {
			return store.ErrConflict
		}
		if c.Meta != nil {
			stored, err := metaRevision(ctx, tx, ref)
			if err != nil {
				return err
			}
			if err := store.CheckMetadata(*c.Meta, cur == 0, stored); err != nil {
				return err
			}
		}
		if c.Write {
			if err := putSpec(ctx, tx, c.Spec, cur); err != nil {
				return err
			}
		}
		if c.Meta != nil {
			if err := putMeta(ctx, tx, ref, *c.Meta); err != nil {
				return err
			}
		}
		spec, obs, err := progress(ctx, tx, ref)
		if err != nil {
			return err
		}
		if c.Write {
			if err := settlePending(ctx, tx, ref, spec, obs, c.Op.CreatedAt); err != nil {
				return err
			}
		}
		op = compute.Settle(c.Op, spec, obs, c.Op.CreatedAt)
		return insertOperation(ctx, tx, op)
	})
	if err != nil {
		return compute.Operation{}, err
	}
	return op, nil
}

func (s *Store) inTx(ctx context.Context, fn func(*sql.Tx) error) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

// progress returns a resource's spec generation (0 if absent) and observed
// state.
func progress(ctx context.Context, q querier, ref compute.Ref) (int64, compute.Observed, error) {
	var (
		gen, observed int64
		code          sql.NullInt64
		msg           sql.NullString
		err           error
	)
	switch ref.Kind {
	case compute.KindInstance:
		err = q.QueryRowContext(ctx, `SELECT generation, observed_generation, failure_code, failure_message
			FROM instances WHERE id = ?`, ref.ID).Scan(&gen, &observed, &code, &msg)
	case compute.KindImage:
		err = q.QueryRowContext(ctx, `SELECT generation, observed_generation, failure_code, failure_message
			FROM images WHERE id = ?`, ref.ID).Scan(&gen, &observed, &code, &msg)
	case compute.KindKeyPair:
		err = q.QueryRowContext(ctx, `SELECT generation FROM key_pairs WHERE id = ?`, ref.ID).Scan(&gen)
		observed = gen
	default:
		return 0, compute.Observed{}, compute.Errorf(compute.CodeInternal, "unknown resource kind %d", ref.Kind)
	}
	if errors.Is(err, sql.ErrNoRows) {
		return 0, compute.Observed{}, nil
	}
	if err != nil {
		return 0, compute.Observed{}, err
	}
	failure, err := failureOf(code, msg)
	return gen, compute.Observed{Generation: observed, Failure: failure}, err
}

// putSpec inserts a new resource when cur is 0 and otherwise advances the
// stored spec from generation cur.
func putSpec(ctx context.Context, tx *sql.Tx, spec compute.Spec, cur int64) error {
	var res sql.Result
	var err error
	switch s := spec.(type) {
	case compute.InstanceSpec:
		var keyName, keyPublic sql.NullString
		if s.Key != nil {
			keyName, keyPublic = nullString(s.Key.Name.String(), true), nullString(s.Key.PublicKey.AuthorizedKey(), true)
		}
		if cur == 0 {
			res, err = tx.ExecContext(ctx, `INSERT INTO instances (id, generation, created_at, changed_at, image_id,
				instance_type, key_name, key_public, power, reboot_epoch) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
				s.ID.String(), s.Generation, nanos(s.CreatedAt), nanos(s.ChangedAt), s.Image.String(),
				s.Type.Name(), keyName, keyPublic, int64(s.Power), int64(s.RebootEpoch))
		} else {
			res, err = tx.ExecContext(ctx, `UPDATE instances SET generation = ?, changed_at = ?, image_id = ?,
				instance_type = ?, key_name = ?, key_public = ?, power = ?, reboot_epoch = ?,
				attempts = 0, retry_at = 0, last_error = ''
				WHERE id = ? AND generation = ?`,
				s.Generation, nanos(s.ChangedAt), s.Image.String(), s.Type.Name(), keyName, keyPublic,
				int64(s.Power), int64(s.RebootEpoch), s.ID.String(), cur)
		}
	case compute.ImageSpec:
		srcURL, srcInstance, srcPath, srcVolume, srcSnapshot, srcErr := imageSourceColumns(s.Source)
		if srcErr != nil {
			return srcErr
		}
		if cur == 0 {
			res, err = tx.ExecContext(ctx, `INSERT INTO images (id, generation, created_at, changed_at, name,
				source_url, source_instance_id, source_path, source_volume, source_snapshot, deregistered)
				VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
				s.ID.String(), s.Generation, nanos(s.CreatedAt), nanos(s.ChangedAt), s.Name.String(),
				srcURL, srcInstance, srcPath, srcVolume, srcSnapshot, boolInt(s.Deregistered))
		} else {
			res, err = tx.ExecContext(ctx, `UPDATE images SET generation = ?, changed_at = ?, name = ?,
				source_url = ?, source_instance_id = ?, source_path = ?, source_volume = ?, source_snapshot = ?,
				deregistered = ?, attempts = 0, retry_at = 0, last_error = ''
				WHERE id = ? AND generation = ?`,
				s.Generation, nanos(s.ChangedAt), s.Name.String(), srcURL, srcInstance, srcPath, srcVolume, srcSnapshot,
				boolInt(s.Deregistered), s.ID.String(), cur)
		}
	case compute.KeyPairSpec:
		if !s.Deleted {
			var taken int
			if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM key_pairs WHERE name = ? AND deleted = 0 AND id <> ?`,
				s.Name.String(), s.ID.String()).Scan(&taken); err != nil {
				return err
			}
			if taken > 0 {
				return store.ErrConflict
			}
		}
		if cur == 0 {
			res, err = tx.ExecContext(ctx, `INSERT INTO key_pairs (id, generation, created_at, changed_at, name,
				public_key, deleted) VALUES (?, ?, ?, ?, ?, ?, ?)`,
				s.ID.String(), s.Generation, nanos(s.CreatedAt), nanos(s.ChangedAt), s.Name.String(),
				s.PublicKey.AuthorizedKey(), boolInt(s.Deleted))
		} else {
			res, err = tx.ExecContext(ctx, `UPDATE key_pairs SET generation = ?, changed_at = ?, name = ?,
				public_key = ?, deleted = ? WHERE id = ? AND generation = ?`,
				s.Generation, nanos(s.ChangedAt), s.Name.String(), s.PublicKey.AuthorizedKey(), boolInt(s.Deleted),
				s.ID.String(), cur)
		}
	default:
		return compute.Errorf(compute.CodeInternal, "unknown spec %T", spec)
	}
	if err != nil {
		return err
	}
	if n, err := res.RowsAffected(); err != nil || n != 1 {
		return errors.Join(store.ErrConflict, err)
	}
	return nil
}

// settlePending applies compute.Settle to every Pending operation on ref.
func settlePending(ctx context.Context, tx *sql.Tx, ref compute.Ref, spec int64, obs compute.Observed, now time.Time) error {
	ops, err := collect(ctx, tx, scanOperation, `SELECT `+operationColumns+` FROM operations
		WHERE target_kind = ? AND target_id = ? AND state = ?`, int64(ref.Kind), ref.ID, int64(compute.OperationPending))
	if err != nil {
		return err
	}
	for _, op := range ops {
		settled := compute.Settle(op, spec, obs, now)
		if !settled.State().Terminal() {
			continue
		}
		code, msg := failureArgs(settled.Failure())
		if _, err := tx.ExecContext(ctx, `UPDATE operations SET state = ?, failure_code = ?, failure_message = ?,
			settled_at = ? WHERE id = ?`, int64(settled.State()), code, msg, nanos(settled.SettledAt()), settled.ID.String()); err != nil {
			return err
		}
	}
	return nil
}

func insertOperation(ctx context.Context, tx *sql.Tx, op compute.Operation) error {
	code, msg := failureArgs(op.Failure())
	_, err := tx.ExecContext(ctx, `INSERT INTO operations (id, request_id, fingerprint, kind, target_kind, target_id,
		target_generation, state, failure_code, failure_message, created_at, settled_at, metadata_only)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		op.ID.String(), op.Request.String(), op.Fingerprint[:], op.Kind.String(), int64(op.Target.Kind), op.Target.ID,
		op.TargetGeneration, int64(op.State()), code, msg, nanos(op.CreatedAt), nanos(op.SettledAt()), boolInt(op.MetadataOnly))
	return err
}

func metaRevision(ctx context.Context, q querier, ref compute.Ref) (int64, error) {
	table, err := tableOf(ref.Kind)
	if err != nil {
		return 0, err
	}
	var rev int64
	err = q.QueryRowContext(ctx, `SELECT meta_revision FROM `+table+` WHERE id = ?`, ref.ID).Scan(&rev)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	return rev, err
}

func tableOf(k compute.Kind) (string, error) {
	switch k {
	case compute.KindInstance:
		return "instances", nil
	case compute.KindImage:
		return "images", nil
	case compute.KindKeyPair:
		return "key_pairs", nil
	default:
		return "", compute.Errorf(compute.CodeInternal, "unknown resource kind %d", k)
	}
}

func putMeta(ctx context.Context, tx *sql.Tx, ref compute.Ref, m compute.Metadata) error {
	table, err := tableOf(ref.Kind)
	if err != nil {
		return err
	}
	labels, err := labelsJSON(m.Labels)
	if err != nil {
		return err
	}
	res, err := tx.ExecContext(ctx, `UPDATE `+table+` SET labels = ?, description = ?, meta_revision = ?, meta_changed_at = ?
		WHERE id = ?`, labels, m.Description.String(), m.Revision, nanos(m.ChangedAt), ref.ID)
	if err != nil {
		return err
	}
	if n, err := res.RowsAffected(); err != nil || n != 1 {
		return errors.Join(store.ErrConflict, err)
	}
	return nil
}

func (s *Store) CommitInstanceStatus(ctx context.Context, id compute.InstanceID, st compute.InstanceStatus) error {
	return s.inTx(ctx, func(tx *sql.Tx) error {
		spec, stored, err := progress(ctx, tx, id.Ref())
		if err != nil {
			return err
		}
		if spec == 0 {
			return compute.Errorf(compute.CodeNotFound, "instance %s not found", id)
		}
		if err := store.CheckObservation(st.Observed, stored, spec, st.UpdatedAt); err != nil {
			return err
		}
		code, msg := failureArgs(st.Observed.Failure)
		if _, err := tx.ExecContext(ctx, `UPDATE instances SET observed_generation = ?, failure_code = ?,
			failure_message = ?, machine = ?, applied_epoch = ?, attempts = ?, retry_at = ?, last_error = ?,
			updated_at = ? WHERE id = ?`,
			st.Observed.Generation, code, msg, int64(st.Machine), int64(st.RebootEpoch), int64(st.Retry.Attempts),
			nanos(st.Retry.At), st.Retry.LastError, nanos(st.UpdatedAt), id.String()); err != nil {
			return err
		}
		return settlePending(ctx, tx, id.Ref(), spec, st.Observed, st.UpdatedAt)
	})
}

func (s *Store) CommitImageStatus(ctx context.Context, id compute.ImageID, st compute.ImageStatus) error {
	return s.inTx(ctx, func(tx *sql.Tx) error {
		spec, stored, err := progress(ctx, tx, id.Ref())
		if err != nil {
			return err
		}
		if spec == 0 {
			return compute.Errorf(compute.CodeNotFound, "image %s not found", id)
		}
		if err := store.CheckObservation(st.Observed, stored, spec, st.UpdatedAt); err != nil {
			return err
		}
		code, msg := failureArgs(st.Observed.Failure)
		if _, err := tx.ExecContext(ctx, `UPDATE images SET observed_generation = ?, failure_code = ?,
			failure_message = ?, present = ?, size_bytes = ?, attempts = ?, retry_at = ?, last_error = ?,
			updated_at = ? WHERE id = ?`,
			st.Observed.Generation, code, msg, boolInt(st.Present), st.SizeBytes, int64(st.Retry.Attempts),
			nanos(st.Retry.At), st.Retry.LastError, nanos(st.UpdatedAt), id.String()); err != nil {
			return err
		}
		return settlePending(ctx, tx, id.Ref(), spec, st.Observed, st.UpdatedAt)
	})
}

func (s *Store) Unconverged(ctx context.Context, now time.Time) ([]compute.Ref, error) {
	return s.lagging(ctx, `generation > observed_generation AND retry_at <= ?`, nanos(now))
}

func (s *Store) Lagging(ctx context.Context) ([]compute.Ref, error) {
	return s.lagging(ctx, `generation > observed_generation`)
}

func (s *Store) lagging(ctx context.Context, where string, args ...any) ([]compute.Ref, error) {
	scanID := func(sc scanner) (string, error) {
		var id string
		return id, sc.Scan(&id)
	}
	refs := []compute.Ref{}
	for _, t := range []struct {
		table string
		kind  compute.Kind
	}{{"instances", compute.KindInstance}, {"images", compute.KindImage}} {
		ids, err := collect(ctx, s.db, scanID, `SELECT id FROM `+t.table+` WHERE `+where+` ORDER BY id`, args...)
		if err != nil {
			return nil, err
		}
		for _, id := range ids {
			refs = append(refs, compute.Ref{Kind: t.kind, ID: id})
		}
	}
	return refs, nil
}
