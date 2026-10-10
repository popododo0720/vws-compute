package sqlitestore

import (
	"database/sql"
	"errors"
	"time"

	"github.com/popododo0720/vws-compute/internal/compute"
)

type scanner interface{ Scan(dest ...any) error }

const instanceColumns = `id, generation, created_at, changed_at, image_id, instance_type, key_name, key_public,
  power, reboot_epoch, observed_generation, failure_code, failure_message, machine, applied_epoch,
  attempts, retry_at, last_error, updated_at`

// instanceRow mirrors one row of the instances table.
type instanceRow struct {
	id, imageID, typeName                               string
	generation, createdAt, changedAt                    int64
	keyName, keyPublic                                  sql.NullString
	power, rebootEpoch                                  int64
	observedGeneration                                  int64
	failureCode                                         sql.NullInt64
	failureMessage                                      sql.NullString
	machine, appliedEpoch, attempts, retryAt, updatedAt int64
	lastError                                           string
}

func scanInstance(sc scanner) (compute.Instance, error) {
	var r instanceRow
	err := sc.Scan(&r.id, &r.generation, &r.createdAt, &r.changedAt, &r.imageID, &r.typeName, &r.keyName, &r.keyPublic,
		&r.power, &r.rebootEpoch, &r.observedGeneration, &r.failureCode, &r.failureMessage, &r.machine, &r.appliedEpoch,
		&r.attempts, &r.retryAt, &r.lastError, &r.updatedAt)
	if err != nil {
		return compute.Instance{}, err
	}
	return r.instance()
}

// instance re-validates every column through compute's parsers, so a
// corrupted row surfaces as an error rather than an invalid domain value.
func (r instanceRow) instance() (compute.Instance, error) {
	id, err1 := compute.ParseInstanceID(r.id)
	img, err2 := compute.ParseImageID(r.imageID)
	typ, err3 := compute.ParseInstanceType(r.typeName)
	failure, err4 := failureOf(r.failureCode, r.failureMessage)
	err5 := inRange("power", r.power, int64(compute.PowerRunning), int64(compute.PowerTerminated))
	err6 := inRange("machine", r.machine, int64(compute.MachineUnknown), int64(compute.MachineOn))
	var key *compute.AuthorizedKey
	var err7 error
	if r.keyName.Valid {
		name, errName := compute.ParseKeyName(r.keyName.String)
		pub, errPub := compute.ParsePublicKey(r.keyPublic.String)
		key, err7 = &compute.AuthorizedKey{Name: name, PublicKey: pub}, errors.Join(errName, errPub)
	}
	if err := errors.Join(err1, err2, err3, err4, err5, err6, err7); err != nil {
		return compute.Instance{}, corrupt("instance", r.id, err)
	}
	return compute.Instance{
		Spec: compute.InstanceSpec{
			ID:          id,
			Generation:  r.generation,
			CreatedAt:   fromNanos(r.createdAt),
			ChangedAt:   fromNanos(r.changedAt),
			Image:       img,
			Type:        typ,
			Key:         key,
			Power:       compute.Power(r.power),
			RebootEpoch: uint32(r.rebootEpoch),
		},
		Status: compute.InstanceStatus{
			Observed:    compute.Observed{Generation: r.observedGeneration, Failure: failure},
			Machine:     compute.MachineState(r.machine),
			RebootEpoch: uint32(r.appliedEpoch),
			Retry:       compute.Retry{Attempts: int(r.attempts), At: fromNanos(r.retryAt), LastError: r.lastError},
			UpdatedAt:   fromNanos(r.updatedAt),
		},
	}, nil
}

const imageColumns = `id, generation, created_at, changed_at, name, source_url, source_instance_id, deregistered,
  observed_generation, failure_code, failure_message, present, size_bytes, attempts, retry_at, last_error, updated_at`

// imageRow mirrors one row of the images table.
type imageRow struct {
	id, name                                         string
	generation, createdAt, changedAt                 int64
	sourceURL, sourceInstanceID                      sql.NullString
	deregistered, observedGeneration                 int64
	failureCode                                      sql.NullInt64
	failureMessage                                   sql.NullString
	present, sizeBytes, attempts, retryAt, updatedAt int64
	lastError                                        string
}

func scanImage(sc scanner) (compute.Image, error) {
	var r imageRow
	err := sc.Scan(&r.id, &r.generation, &r.createdAt, &r.changedAt, &r.name, &r.sourceURL, &r.sourceInstanceID, &r.deregistered,
		&r.observedGeneration, &r.failureCode, &r.failureMessage, &r.present, &r.sizeBytes, &r.attempts, &r.retryAt, &r.lastError, &r.updatedAt)
	if err != nil {
		return compute.Image{}, err
	}
	return r.image()
}

func (r imageRow) image() (compute.Image, error) {
	id, err1 := compute.ParseImageID(r.id)
	name, err2 := compute.ParseImageName(r.name)
	failure, err3 := failureOf(r.failureCode, r.failureMessage)
	var src compute.ImageSource
	var err4 error
	switch {
	case r.sourceURL.Valid:
		var u compute.ImageURL
		u, err4 = compute.ParseImageURL(r.sourceURL.String)
		src = compute.FromURL{URL: u}
	case r.sourceInstanceID.Valid:
		var inst compute.InstanceID
		inst, err4 = compute.ParseInstanceID(r.sourceInstanceID.String)
		src = compute.FromInstance{Instance: inst}
	default:
		err4 = errors.New("no image source")
	}
	if err := errors.Join(err1, err2, err3, err4); err != nil {
		return compute.Image{}, corrupt("image", r.id, err)
	}
	return compute.Image{
		Spec: compute.ImageSpec{
			ID:           id,
			Generation:   r.generation,
			CreatedAt:    fromNanos(r.createdAt),
			ChangedAt:    fromNanos(r.changedAt),
			Name:         name,
			Source:       src,
			Deregistered: r.deregistered != 0,
		},
		Status: compute.ImageStatus{
			Observed:  compute.Observed{Generation: r.observedGeneration, Failure: failure},
			Present:   r.present != 0,
			SizeBytes: r.sizeBytes,
			Retry:     compute.Retry{Attempts: int(r.attempts), At: fromNanos(r.retryAt), LastError: r.lastError},
			UpdatedAt: fromNanos(r.updatedAt),
		},
	}, nil
}

const keyPairColumns = `id, generation, created_at, changed_at, name, public_key, deleted`

// keyPairRow mirrors one row of the key_pairs table.
type keyPairRow struct {
	id, name, publicKey                       string
	generation, createdAt, changedAt, deleted int64
}

func scanKeyPair(sc scanner) (compute.KeyPair, error) {
	var r keyPairRow
	if err := sc.Scan(&r.id, &r.generation, &r.createdAt, &r.changedAt, &r.name, &r.publicKey, &r.deleted); err != nil {
		return compute.KeyPair{}, err
	}
	return r.keyPair()
}

func (r keyPairRow) keyPair() (compute.KeyPair, error) {
	id, err1 := compute.ParseKeyPairID(r.id)
	name, err2 := compute.ParseKeyName(r.name)
	pub, err3 := compute.ParsePublicKey(r.publicKey)
	if err := errors.Join(err1, err2, err3); err != nil {
		return compute.KeyPair{}, corrupt("key pair", r.id, err)
	}
	return compute.KeyPair{Spec: compute.KeyPairSpec{
		ID:         id,
		Generation: r.generation,
		CreatedAt:  fromNanos(r.createdAt),
		ChangedAt:  fromNanos(r.changedAt),
		Name:       name,
		PublicKey:  pub,
		Deleted:    r.deleted != 0,
	}}, nil
}

const operationColumns = `id, request_id, fingerprint, kind, target_kind, target_id, target_generation, state,
  failure_code, failure_message, created_at, settled_at`

// operationRow mirrors one row of the operations table.
type operationRow struct {
	id, requestID, kind, targetID       string
	fingerprint                         []byte
	targetKind, targetGeneration, state int64
	failureCode                         sql.NullInt64
	failureMessage                      sql.NullString
	createdAt, settledAt                int64
}

func scanOperation(sc scanner) (compute.Operation, error) {
	var r operationRow
	err := sc.Scan(&r.id, &r.requestID, &r.fingerprint, &r.kind, &r.targetKind, &r.targetID, &r.targetGeneration, &r.state,
		&r.failureCode, &r.failureMessage, &r.createdAt, &r.settledAt)
	if err != nil {
		return compute.Operation{}, err
	}
	return r.operation()
}

// operation is the only caller of compute.RestoreOperation.
func (r operationRow) operation() (compute.Operation, error) {
	id, err1 := compute.ParseOperationID(r.id)
	req, err2 := compute.ParseRequestID(r.requestID)
	kind, err3 := compute.ParseOperationKind(r.kind)
	failure, err4 := failureOf(r.failureCode, r.failureMessage)
	err5 := inRange("target kind", r.targetKind, int64(compute.KindInstance), int64(compute.KindKeyPair))
	var err6 error
	if len(r.fingerprint) != len(compute.Fingerprint{}) {
		err6 = errors.New("fingerprint has the wrong length")
	}
	if err := errors.Join(err1, err2, err3, err4, err5, err6); err != nil {
		return compute.Operation{}, corrupt("operation", r.id, err)
	}
	base := compute.Operation{
		ID:               id,
		Request:          req,
		Kind:             kind,
		Target:           compute.Ref{Kind: compute.Kind(r.targetKind), ID: r.targetID},
		TargetGeneration: r.targetGeneration,
		CreatedAt:        fromNanos(r.createdAt),
	}
	copy(base.Fingerprint[:], r.fingerprint)
	return compute.RestoreOperation(base, compute.OperationState(r.state), failure, fromNanos(r.settledAt))
}

func failureOf(code sql.NullInt64, msg sql.NullString) (*compute.Failure, error) {
	if !code.Valid {
		return nil, nil
	}
	if err := inRange("failure code", code.Int64, int64(compute.CodeInvalid), int64(compute.CodeInternal)); err != nil {
		return nil, err
	}
	return &compute.Failure{Code: compute.Code(code.Int64), Message: msg.String}, nil
}

func failureArgs(f *compute.Failure) (sql.NullInt64, sql.NullString) {
	if f == nil {
		return sql.NullInt64{}, sql.NullString{}
	}
	return sql.NullInt64{Int64: int64(f.Code), Valid: true}, sql.NullString{String: f.Message, Valid: true}
}

func inRange(field string, v, lo, hi int64) error {
	if v < lo || v > hi {
		return compute.Errorf(compute.CodeInternal, "%s %d out of range", field, v)
	}
	return nil
}

func corrupt(what, id string, err error) error {
	return compute.Errorf(compute.CodeInternal, "corrupt %s row %q: %v", what, id, err)
}

// nanos stores the zero time as 0 so that "no retry scheduled" sorts before
// every real time.
func nanos(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixNano()
}

func fromNanos(n int64) time.Time {
	if n == 0 {
		return time.Time{}
	}
	return time.Unix(0, n).UTC()
}

func boolInt(b bool) int64 {
	if b {
		return 1
	}
	return 0
}

func nullString(s string, valid bool) sql.NullString { return sql.NullString{String: s, Valid: valid} }
