package compute

import (
	"fmt"
	"time"
)

// OperationKind names the intent an operation tracks. String values are
// persisted and fingerprinted, so they must never change.
type OperationKind uint8

const (
	OpRunInstance OperationKind = iota + 1
	OpStartInstance
	OpStopInstance
	OpRebootInstance
	OpTerminateInstance
	OpRegisterImage
	OpCreateImage
	OpDeregisterImage
	OpCreateKeyPair
	OpImportKeyPair
	OpDeleteKeyPair
	OpUpdateInstanceMetadata
	OpUpdateImageMetadata
	OpUpdateKeyPairMetadata
	OpCopyImage
	OpRegisterFile
	OpCreateInstance
)

var operationKindNames = [...]string{
	OpRunInstance:            "RunInstance",
	OpStartInstance:          "StartInstance",
	OpStopInstance:           "StopInstance",
	OpRebootInstance:         "RebootInstance",
	OpTerminateInstance:      "TerminateInstance",
	OpRegisterImage:          "RegisterImage",
	OpCreateImage:            "CreateImage",
	OpDeregisterImage:        "DeregisterImage",
	OpCreateKeyPair:          "CreateKeyPair",
	OpImportKeyPair:          "ImportKeyPair",
	OpDeleteKeyPair:          "DeleteKeyPair",
	OpUpdateInstanceMetadata: "UpdateInstanceMetadata",
	OpUpdateImageMetadata:    "UpdateImageMetadata",
	OpUpdateKeyPairMetadata:  "UpdateKeyPairMetadata",
	OpCopyImage:              "CopyImage",
	OpRegisterFile:           "RegisterFile",
	OpCreateInstance:         "CreateInstance",
}

// OperationKinds returns every kind in declaration order.
func OperationKinds() []OperationKind {
	ks := make([]OperationKind, 0, len(operationKindNames)-1)
	for k := OpRunInstance; int(k) < len(operationKindNames); k++ {
		ks = append(ks, k)
	}
	return ks
}

func (k OperationKind) String() string {
	if k >= OpRunInstance && int(k) < len(operationKindNames) {
		return operationKindNames[k]
	}
	return fmt.Sprintf("OperationKind(%d)", k)
}

// ParseOperationKind inverts String.
func ParseOperationKind(s string) (OperationKind, error) {
	for _, k := range OperationKinds() {
		if k.String() == s {
			return k, nil
		}
	}
	return 0, Errorf(CodeInvalid, "unknown operation kind %q", s)
}

// OperationState is a node of the operation state machine.
type OperationState uint8

const (
	OperationPending OperationState = iota + 1
	OperationSucceeded
	OperationFailed
	OperationSuperseded
)

// Terminal reports whether s is absorbing.
func (s OperationState) Terminal() bool { return s != OperationPending }

func (s OperationState) String() string {
	switch s {
	case OperationPending:
		return "Pending"
	case OperationSucceeded:
		return "Succeeded"
	case OperationFailed:
		return "Failed"
	case OperationSuperseded:
		return "Superseded"
	}
	return fmt.Sprintf("OperationState(%d)", s)
}

// Operation is the durable record of one admitted request.
//
// It is also the idempotency record. Request is unique across stored
// operations, so "have I seen this request?" and "what happened to it?" are
// one lookup and cannot disagree.
//
// An Operation targets exactly one (resource, generation) pair. It leaves
// Pending only through Settle:
//
//	Pending --MetadataOnly----------------------------> Succeeded
//	Pending --observed == target, no failure--------> Succeeded
//	Pending --observed == target, failure-----------> Failed
//	Pending --observed > target, or spec > target---> Superseded
//
// Terminal states are absorbing. The state fields are unexported, so no code
// outside this package can move an operation except through Settle.
type Operation struct {
	ID               OperationID
	Request          RequestID
	Fingerprint      Fingerprint
	Kind             OperationKind
	Target           Ref
	TargetGeneration int64
	// MetadataOnly marks a request that carried metadata and wrote no spec
	// generation. Settle succeeds it the first time it runs, inside
	// CommitDesired, so it does not wait on the agent. NewOperation sets it
	// once and it never changes.
	MetadataOnly bool
	CreatedAt    time.Time

	state     OperationState
	failure   *Failure
	settledAt time.Time
}

// NewOperation builds the Pending operation for an admitted change. It mints
// the ID from env.Rand and takes Target and TargetGeneration from ch.Spec,
// so the operation cannot point at a different resource than the change.
func NewOperation(env Env, req RequestID, in Intent, ch Change) (Operation, error) {
	if req.IsZero() || in == nil || ch.Spec == nil || env.Now.IsZero() {
		return Operation{}, Errorf(CodeInternal, "operation needs a request id, an intent, a change, and a time")
	}
	id, err := newOperationID(env.Rand)
	if err != nil {
		return Operation{}, err
	}
	return Operation{
		ID:               id,
		Request:          req,
		Fingerprint:      FingerprintOf(in),
		Kind:             in.Kind(),
		Target:           ch.Spec.Ref(),
		TargetGeneration: ch.Spec.Gen(),
		MetadataOnly:     !ch.Write && in.carriesMetadata(),
		CreatedAt:        env.Now,
		state:            OperationPending,
	}, nil
}

func (o Operation) State() OperationState { return o.state }

// Failure is non-nil iff State is OperationFailed. It returns a copy.
func (o Operation) Failure() *Failure {
	if o.failure == nil {
		return nil
	}
	f := *o.failure
	return &f
}

// SettledAt is non-zero iff State is terminal.
func (o Operation) SettledAt() time.Time { return o.settledAt }

// Settle advances o against its target's current spec generation and
// observed state. It is the only transition function. Every store calls it
// inside the transaction that writes either side of the target, which keeps
// the repository invariant
//
//	no Pending operation has TargetGeneration <= observed generation,
//	and none has TargetGeneration < spec generation.
func Settle(o Operation, spec int64, obs Observed, now time.Time) Operation {
	if o.state != OperationPending {
		return o
	}
	switch {
	case o.MetadataOnly:
		o.state = OperationSucceeded
	case obs.Generation > o.TargetGeneration:
		o.state = OperationSuperseded
	case obs.Generation == o.TargetGeneration && obs.Failure != nil:
		f := *obs.Failure
		o.state, o.failure = OperationFailed, &f
	case obs.Generation == o.TargetGeneration:
		o.state = OperationSucceeded
	case spec > o.TargetGeneration:
		o.state = OperationSuperseded
	default:
		return o
	}
	o.settledAt = now
	return o
}

// RestoreOperation rebuilds an Operation read back from storage. It is not a
// transition: it rejects records that Settle could not have produced (a
// failure without state Failed, a terminal state without settledAt).
func RestoreOperation(base Operation, state OperationState, failure *Failure, settledAt time.Time) (Operation, error) {
	switch {
	case base.ID.IsZero() || base.Request.IsZero():
		return Operation{}, Errorf(CodeInternal, "stored operation lacks an id or request id")
	case state < OperationPending || state > OperationSuperseded:
		return Operation{}, Errorf(CodeInternal, "operation %s: invalid state %d", base.ID, state)
	case state.Terminal() == settledAt.IsZero():
		return Operation{}, Errorf(CodeInternal, "operation %s: state %s with settled time %v", base.ID, state, settledAt)
	case (state == OperationFailed) != (failure != nil):
		return Operation{}, Errorf(CodeInternal, "operation %s: state %s with failure %v", base.ID, state, failure)
	case base.MetadataOnly && state != OperationSucceeded:
		return Operation{}, Errorf(CodeInternal, "operation %s: metadata-only record is %s", base.ID, state)
	}
	base.state, base.settledAt = state, settledAt
	if failure != nil {
		f := *failure
		base.failure = &f
	}
	return base, nil
}
