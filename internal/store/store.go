// Package store defines the repository: the single owner of desired state,
// observed status, and operations.
//
// The repository is split by writer. DesiredStore goes to the controller and
// StatusStore to the reconciler. Only cmd/vws-compute holds a whole
// Repository. Each method commits atomically and keeps the invariants in its
// doc comment. storetest.Run checks every implementation against one suite.
//
// Values crossing this interface are compute types. Storage rows are
// unexported inside each implementation.
package store

import (
	"context"
	"errors"
	"time"

	"github.com/popododo0720/vws-compute/internal/compute"
)

// ErrConflict reports a failed precondition: state changed after the plan
// read it. The controller replans. RPC callers never see it.
var ErrConflict = errors.New("store: conflict")

// ErrStale reports an observation older than the stored one. The reconciler
// drops the result.
var ErrStale = errors.New("store: stale observation")

// Version is a resource generation that a plan depended on. Generation 0
// means "did not exist".
type Version struct {
	Ref        compute.Ref
	Generation int64
}

// Page is one page of a listing, ordered by ID (key pairs by name). Next is
// the After value for the following page and is empty on the last page.
type Page[T any] struct {
	Items []T
	Next  string
}

const (
	DefaultPageSize = 100
	MaxPageSize     = 1000
)

// PageLimit clamps a requested page size into [1, MaxPageSize], with 0
// meaning DefaultPageSize.
func PageLimit(n int) int {
	switch {
	case n <= 0:
		return DefaultPageSize
	case n > MaxPageSize:
		return MaxPageSize
	}
	return n
}

// InstanceQuery selects instances with ID greater than After. Empty IDs
// means all retained instances, including terminated ones.
type InstanceQuery struct {
	IDs      []compute.InstanceID
	PageSize int
	After    string
}

// ImageQuery selects images with ID greater than After. Empty IDs means all
// retained images, including deregistered ones.
type ImageQuery struct {
	IDs      []compute.ImageID
	PageSize int
	After    string
}

// KeyPairQuery selects live key pairs with name greater than After. Empty
// Names means all live key pairs.
type KeyPairQuery struct {
	Names    []compute.KeyName
	PageSize int
	After    string
}

// Reader reads merged desired and observed state. Every returned value is a
// copy that shares no memory with the store. Missing resources fail with
// compute.CodeNotFound.
type Reader interface {
	Instance(ctx context.Context, id compute.InstanceID) (compute.Instance, error)
	Instances(ctx context.Context, q InstanceQuery) (Page[compute.Instance], error)
	Image(ctx context.Context, id compute.ImageID) (compute.Image, error)
	Images(ctx context.Context, q ImageQuery) (Page[compute.Image], error)
	KeyPair(ctx context.Context, id compute.KeyPairID) (compute.KeyPair, error)
	// KeyPairByName returns the live key pair holding name or, when none is
	// live, the most recently changed tombstone with that name.
	KeyPairByName(ctx context.Context, name compute.KeyName) (compute.KeyPair, error)
	KeyPairs(ctx context.Context, q KeyPairQuery) (Page[compute.KeyPair], error)
	Operation(ctx context.Context, id compute.OperationID) (compute.Operation, error)
	OperationByRequest(ctx context.Context, req compute.RequestID) (compute.Operation, error)
}

// DesiredChange is one admitted intent, ready to commit.
type DesiredChange struct {
	// Op is the Pending operation from compute.NewOperation.
	Op compute.Operation
	// Spec and Write come from compute.Change. There is deliberately no field
	// for the change's private key.
	Spec  compute.Spec
	Write bool
	// Expect is every version the plan read.
	Expect []Version
}

// DesiredStore is the controller's view of the repository.
type DesiredStore interface {
	Reader

	// CommitDesired atomically does one of the following:
	//
	//  1. If an operation with c.Op.Request exists: return it unchanged if
	//     its Fingerprint equals c.Op.Fingerprint, or fail with
	//     compute.CodeRequestMismatch. Nothing is written.
	//  2. Otherwise, if any Expect version differs from storage, or a second
	//     live key pair would hold a name: fail with ErrConflict. Nothing is
	//     written.
	//  3. Otherwise: if c.Write, persist c.Spec, whose generation must be
	//     stored+1 (1 for a new resource, which also creates its empty
	//     status), and Settle every Pending operation on the target against
	//     the new spec generation. If !c.Write, require the stored
	//     generation to equal c.Spec.Gen(). Then insert
	//     compute.Settle(c.Op, specGen, observed, c.Op.CreatedAt) and return
	//     it.
	//
	// Step 3's Settle is what lets a no-op on a converged resource, and every
	// key pair operation, come back already Succeeded, and what supersedes
	// operations that a newer desired generation replaced.
	CommitDesired(ctx context.Context, c DesiredChange) (compute.Operation, error)
}

// StatusStore is the reconciler's view of the repository.
type StatusStore interface {
	Reader

	// CommitInstanceStatus persists st and, in the same transaction, applies
	// compute.Settle at st.UpdatedAt to every Pending operation targeting id.
	// It fails with ErrStale if st.Observed.Generation is below the stored
	// one, and with compute.CodeInternal if it is above the spec's generation
	// or st.UpdatedAt is zero. Writing the same generation again is allowed:
	// that is how retry state lands.
	CommitInstanceStatus(ctx context.Context, id compute.InstanceID, st compute.InstanceStatus) error
	// CommitImageStatus is CommitInstanceStatus for images.
	CommitImageStatus(ctx context.Context, id compute.ImageID, st compute.ImageStatus) error

	// Unconverged lists, instances first and each kind by ID, every instance
	// and image whose spec generation is ahead of its observed generation
	// and whose retry time is not after now.
	Unconverged(ctx context.Context, now time.Time) ([]compute.Ref, error)
	// Lagging is Unconverged without the retry-time filter. The reconciler
	// uses it to hold a destroy back while a capture that is still backing
	// off still needs the source machine.
	Lagging(ctx context.Context) ([]compute.Ref, error)
}

// CheckObservation is the rule every Commit*Status applies before it writes:
// compute.CodeInternal if the observation has no time (operations it settles
// would carry a zero settled time), ErrStale if next is behind stored, and
// compute.CodeInternal if next is ahead of the spec generation.
func CheckObservation(next, stored compute.Observed, spec int64, at time.Time) error {
	switch {
	case at.IsZero():
		return compute.Errorf(compute.CodeInternal, "observation at generation %d has no time", next.Generation)
	case next.Generation < stored.Generation:
		return ErrStale
	case next.Generation > spec:
		return compute.Errorf(compute.CodeInternal, "observed generation %d is ahead of spec generation %d", next.Generation, spec)
	}
	return nil
}

// Repository is the whole store. Only cmd/vws-compute and tests hold one.
type Repository interface {
	DesiredStore
	StatusStore
	Close() error
}
