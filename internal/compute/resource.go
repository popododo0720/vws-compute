package compute

import (
	"fmt"
	"slices"
	"time"
)

// Kind names a resource family.
type Kind uint8

const (
	KindInstance Kind = iota + 1
	KindImage
	KindKeyPair
)

func (k Kind) String() string {
	switch k {
	case KindInstance:
		return "instance"
	case KindImage:
		return "image"
	case KindKeyPair:
		return "keypair"
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
	// AttachmentFailure is a permanent failure to attach or detach a volume
	// at Generation. It fails the operation that asked for the change but
	// does not park the instance, which keeps running as on EC2.
	AttachmentFailure *AttachmentFailure
}

// Parked reports whether the reconciler gave up on generation gen. A parked
// resource is not retried until a new intent advances its generation. An
// AttachmentFailure does not park an instance.
func (o Observed) Parked(gen int64) bool { return o.Generation == gen && o.Failure != nil }

// LaunchReason is why a launch parked. Zero is LaunchInternal, which is what
// a stored failure loads when it has no reason.
type LaunchReason uint8

const (
	LaunchInternal LaunchReason = iota
	LaunchImageNotFound
)

var launchTokens = [...]string{
	LaunchInternal:      "Internal",
	LaunchImageNotFound: "ImageNotFound",
}

// Token is the wire string for r.
func (r LaunchReason) Token() string {
	if int(r) < len(launchTokens) && launchTokens[r] != "" {
		return launchTokens[r]
	}
	return launchTokens[LaunchInternal]
}

// ParseLaunchReason returns LaunchInternal for any token other than the known ones.
func ParseLaunchReason(token string) LaunchReason {
	for i, s := range launchTokens {
		if s == token {
			return LaunchReason(i)
		}
	}
	return LaunchInternal
}

// Failure explains why a generation could not be converged. Settle copies it
// into every operation that fails at that generation.
type Failure struct {
	Code    Code
	Message string
	Reason  LaunchReason
}

// AttachmentFailure is a permanent refusal to attach or detach volumes. It
// fails the operation that asked for the change without parking the instance.
// The next intent drops the refused volumes from desired state, as EC2 leaves
// no attachment behind when AttachVolume fails. A create can refuse more than
// one data volume, so Volumes lists every refused one.
type AttachmentFailure struct {
	Failure
	Volumes []VolumeRef
}

// Refused reports whether f refused v. A nil f refused nothing.
func (f *AttachmentFailure) Refused(v VolumeRef) bool {
	return f != nil && slices.Contains(f.Volumes, v)
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
