package sqlitestore

import (
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"github.com/popododo0720/vws-compute/internal/compute"
)

type scanner interface{ Scan(dest ...any) error }

const metaColumns = `labels, description, meta_revision, meta_changed_at`

const instanceColumns = `id, generation, created_at, changed_at, image_id, instance_type, key_name, key_public,
  power, reboot_epoch, observed_generation, failure_code, failure_message, machine, applied_epoch,
  attempts, retry_at, last_error, updated_at, ` + metaColumns + `,
  node_driven, project, zone, user_data, volumes, nics, assigned_node, report`

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
	meta                                                metaRow
	nodeDriven                                          int64
	project, zone, userData, volumes, nics              string
	assignedNode, report                                string
}

func scanInstance(sc scanner) (compute.Instance, error) {
	var r instanceRow
	err := sc.Scan(append([]any{&r.id, &r.generation, &r.createdAt, &r.changedAt, &r.imageID, &r.typeName, &r.keyName, &r.keyPublic,
		&r.power, &r.rebootEpoch, &r.observedGeneration, &r.failureCode, &r.failureMessage, &r.machine, &r.appliedEpoch,
		&r.attempts, &r.retryAt, &r.lastError, &r.updatedAt}, append(r.meta.dest(),
		&r.nodeDriven, &r.project, &r.zone, &r.userData, &r.volumes, &r.nics, &r.assignedNode, &r.report)...)...)
	if err != nil {
		return compute.Instance{}, err
	}
	return r.instance()
}

// instance re-validates every column through compute's parsers, so a
// corrupted row surfaces as an error rather than an invalid domain value.
func (r instanceRow) instance() (compute.Instance, error) {
	id, err1 := compute.ParseInstanceID(r.id)
	var img compute.ImageID
	var err2 error
	if r.nodeDriven == 0 {
		img, err2 = compute.ParseImageID(r.imageID)
	}
	typ, err3 := compute.ParseMachineType(r.typeName)
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
	meta, err8 := r.meta.metadata()
	vols, err9 := parseVolumesJSON(r.volumes)
	nics, err10 := parseNICsJSON(r.nics)
	report, err11 := parseReportJSON(r.report)
	if err := errors.Join(err1, err2, err3, err4, err5, err6, err7, err8, err9, err10, err11); err != nil {
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
			Project:     r.project,
			Zone:        r.zone,
			UserData:    r.userData,
			Volumes:     vols,
			NICs:        nics,
		},
		Status: compute.InstanceStatus{
			Observed:    compute.Observed{Generation: r.observedGeneration, Failure: failure},
			Machine:     compute.MachineState(r.machine),
			RebootEpoch: uint32(r.appliedEpoch),
			Retry:       compute.Retry{Attempts: int(r.attempts), At: fromNanos(r.retryAt), LastError: r.lastError},
			UpdatedAt:   fromNanos(r.updatedAt),

			AssignedNode: r.assignedNode,
			Report:       report,
		},
		Meta: meta,
	}, nil
}

const imageColumns = `id, generation, created_at, changed_at, name, source_url, source_instance_id,
  source_path, source_volume, source_snapshot, deregistered,
  observed_generation, failure_code, failure_message, present, size_bytes, attempts, retry_at, last_error, updated_at, ` + metaColumns

// imageRow mirrors one row of the images table.
type imageRow struct {
	id, name                                         string
	generation, createdAt, changedAt                 int64
	sourceURL, sourceInstanceID                      sql.NullString
	sourcePath, sourceVolume, sourceSnapshot         sql.NullString
	deregistered, observedGeneration                 int64
	failureCode                                      sql.NullInt64
	failureMessage                                   sql.NullString
	present, sizeBytes, attempts, retryAt, updatedAt int64
	lastError                                        string
	meta                                             metaRow
}

func scanImage(sc scanner) (compute.Image, error) {
	var r imageRow
	err := sc.Scan(append([]any{&r.id, &r.generation, &r.createdAt, &r.changedAt, &r.name, &r.sourceURL, &r.sourceInstanceID,
		&r.sourcePath, &r.sourceVolume, &r.sourceSnapshot, &r.deregistered,
		&r.observedGeneration, &r.failureCode, &r.failureMessage, &r.present, &r.sizeBytes, &r.attempts, &r.retryAt, &r.lastError, &r.updatedAt},
		r.meta.dest()...)...)
	if err != nil {
		return compute.Image{}, err
	}
	return r.image()
}

func (r imageRow) image() (compute.Image, error) {
	id, err1 := compute.ParseImageID(r.id)
	var name compute.ImageName
	var err2 error
	if r.name != "" {
		name, err2 = compute.ParseImageName(r.name)
	}
	failure, err3 := failureOf(r.failureCode, r.failureMessage)
	src, err4 := imageSource(r)
	meta, err5 := r.meta.metadata()
	if err := errors.Join(err1, err2, err3, err4, err5); err != nil {
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
		Meta: meta,
	}, nil
}

func imageSource(r imageRow) (compute.ImageSource, error) {
	n := 0
	var src compute.ImageSource
	var err error
	set := func(e error, next compute.ImageSource) {
		n++
		if e != nil {
			err = e
			return
		}
		src = next
	}
	if r.sourceURL.Valid {
		u, e := compute.ParseImageURL(r.sourceURL.String)
		set(e, compute.FromURL{URL: u})
	}
	if r.sourceInstanceID.Valid {
		inst, e := compute.ParseInstanceID(r.sourceInstanceID.String)
		set(e, compute.FromInstance{Instance: inst})
	}
	if r.sourcePath.Valid {
		p, e := compute.ParseImagePath(r.sourcePath.String)
		set(e, compute.FromFile{Path: p})
	}
	if r.sourceVolume.Valid {
		ref, e := compute.ParseVolumeRef(r.sourceVolume.String)
		set(e, compute.FromVolume{Ref: ref})
	}
	if r.sourceSnapshot.Valid {
		ref, e := compute.ParseSnapshotRef(r.sourceSnapshot.String)
		set(e, compute.FromSnapshot{Ref: ref})
	}
	if err != nil {
		return nil, err
	}
	if n != 1 {
		return nil, errors.New("image source count")
	}
	return src, nil
}

const keyPairColumns = `id, generation, created_at, changed_at, name, public_key, deleted, ` + metaColumns

// keyPairRow mirrors one row of the key_pairs table.
type keyPairRow struct {
	id, name, publicKey                       string
	generation, createdAt, changedAt, deleted int64
	meta                                      metaRow
}

func scanKeyPair(sc scanner) (compute.KeyPair, error) {
	var r keyPairRow
	if err := sc.Scan(append([]any{&r.id, &r.generation, &r.createdAt, &r.changedAt, &r.name, &r.publicKey, &r.deleted}, r.meta.dest()...)...); err != nil {
		return compute.KeyPair{}, err
	}
	return r.keyPair()
}

func (r keyPairRow) keyPair() (compute.KeyPair, error) {
	id, err1 := compute.ParseKeyPairID(r.id)
	name, err2 := compute.ParseKeyName(r.name)
	pub, err3 := compute.ParsePublicKey(r.publicKey)
	meta, err4 := r.meta.metadata()
	if err := errors.Join(err1, err2, err3, err4); err != nil {
		return compute.KeyPair{}, corrupt("key pair", r.id, err)
	}
	return compute.KeyPair{
		Spec: compute.KeyPairSpec{
			ID:         id,
			Generation: r.generation,
			CreatedAt:  fromNanos(r.createdAt),
			ChangedAt:  fromNanos(r.changedAt),
			Name:       name,
			PublicKey:  pub,
			Deleted:    r.deleted != 0,
		},
		Meta: meta,
	}, nil
}

const operationColumns = `id, request_id, fingerprint, kind, target_kind, target_id, target_generation, state,
  failure_code, failure_message, created_at, settled_at, metadata_only`

// operationRow mirrors one row of the operations table.
type operationRow struct {
	id, requestID, kind, targetID       string
	fingerprint                         []byte
	targetKind, targetGeneration, state int64
	failureCode                         sql.NullInt64
	failureMessage                      sql.NullString
	createdAt, settledAt, metadataOnly  int64
}

func scanOperation(sc scanner) (compute.Operation, error) {
	var r operationRow
	err := sc.Scan(&r.id, &r.requestID, &r.fingerprint, &r.kind, &r.targetKind, &r.targetID, &r.targetGeneration, &r.state,
		&r.failureCode, &r.failureMessage, &r.createdAt, &r.settledAt, &r.metadataOnly)
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
	err5 := inRange("target kind", r.targetKind, int64(compute.KindInstance), int64(compute.KindNode))
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
		MetadataOnly:     r.metadataOnly != 0,
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

type metaRow struct {
	labels, description string
	revision, changedAt int64
}

func (r *metaRow) dest() []any {
	return []any{&r.labels, &r.description, &r.revision, &r.changedAt}
}

func (r metaRow) metadata() (compute.Metadata, error) {
	labels, err1 := parseLabelsJSON(r.labels)
	desc, err2 := compute.ParseDescription(r.description)
	err3 := inRange("meta revision", r.revision, 0, 1<<62)
	var err4 error
	if (r.revision == 0) != (r.changedAt == 0) {
		err4 = errors.New("metadata revision and change time disagree")
	}
	if err := errors.Join(err1, err2, err3, err4); err != nil {
		return compute.Metadata{}, err
	}
	return compute.Metadata{
		Labels:      labels,
		Description: desc,
		Revision:    r.revision,
		ChangedAt:   fromNanos(r.changedAt),
	}, nil
}

func labelsJSON(l compute.Labels) (string, error) {
	if l.Len() == 0 {
		return "{}", nil
	}
	b, err := json.Marshal(l.Map())
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func parseLabelsJSON(s string) (compute.Labels, error) {
	var m map[string]string
	if err := json.Unmarshal([]byte(s), &m); err != nil {
		return compute.Labels{}, err
	}
	return compute.ParseLabels(m)
}

func boolInt(b bool) int64 {
	if b {
		return 1
	}
	return 0
}

func nullString(s string, valid bool) sql.NullString { return sql.NullString{String: s, Valid: valid} }

func imageSourceColumns(src compute.ImageSource) (url, instance, path, volume, snapshot sql.NullString, err error) {
	switch src := src.(type) {
	case compute.FromURL:
		url = nullString(src.URL.String(), true)
	case compute.FromInstance:
		instance = nullString(src.Instance.String(), true)
	case compute.FromFile:
		path = nullString(src.Path.String(), true)
	case compute.FromVolume:
		volume = nullString(src.Ref.String(), true)
	case compute.FromSnapshot:
		snapshot = nullString(src.Ref.String(), true)
	default:
		err = compute.Errorf(compute.CodeInternal, "image has no source")
	}
	return url, instance, path, volume, snapshot, err
}
