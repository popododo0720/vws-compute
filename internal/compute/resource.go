package compute

import (
	"fmt"
	"time"
)

// Kind names a resource family.
type Kind uint8

const (
	KindInstance Kind = iota + 1
	KindImage
	KindKeyPair
	KindNode
)

func (k Kind) String() string {
	switch k {
	case KindInstance:
		return "instance"
	case KindImage:
		return "image"
	case KindKeyPair:
		return "keypair"
	case KindNode:
		return "node"
	}
	return fmt.Sprintf("Kind(%d)", k)
}

// Ref points at one resource of any kind. It is an operation's target and
// the reconciler's unit of work. ID is the String form of the typed ID.
type Ref struct {
	Kind Kind
	ID   string
}

func (r Ref) String() string { return r.Kind.String() + "/" + r.ID }

// Spec is a resource's desired state at one generation. Only Plan produces
// Specs and only store.DesiredStore persists them. Status types do not
// implement Spec, so an observation cannot be committed as desired state.
type Spec interface {
	Ref() Ref
	// Gen is the generation this spec is, or becomes when committed.
	Gen() int64
	isSpec()
}

// Observed is the reconciler's verdict on the most recent generation it
// finished with.
//
// Invariant: 0 <= Generation <= the spec's Generation. Generation 0 means
// never observed. Generation moves forward only, and only when the
// reconciler converged that generation or gave up on it (Failure != nil).
type Observed struct {
	Generation int64
	Failure    *Failure
}

// Parked reports whether the reconciler gave up on generation gen. A parked
// resource is not retried until a new intent advances its generation.
func (o Observed) Parked(gen int64) bool { return o.Generation == gen && o.Failure != nil }

// Failure explains why a generation could not be converged. Settle copies it
// into every operation that fails at that generation.
type Failure struct {
	Code    Code
	Message string
}

// Retry is the reconciler's durable backoff after transient agent errors.
// It lives on the status row, so a restart resumes the same schedule. The
// zero value means no transient failure is outstanding.
type Retry struct {
	Attempts  int
	At        time.Time
	LastError string
}

// Due reports whether the reconciler may call the agent at now.
func (r Retry) Due(now time.Time) bool { return !r.At.After(now) }
