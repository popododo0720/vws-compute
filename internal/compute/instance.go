package compute

import "time"

// Power is an instance's desired power state. PowerTerminated is absorbing:
// no intent moves an instance out of it.
type Power uint8

const (
	PowerRunning Power = iota + 1
	PowerStopped
	PowerTerminated
)

// InstanceSpec is the desired state of one instance at one generation.
type InstanceSpec struct {
	ID InstanceID
	// Generation is 1 at RunInstance and advances by exactly 1 on every
	// committed desired change.
	Generation int64
	CreatedAt  time.Time
	// ChangedAt is when Generation last advanced. The reconciler's
	// transient-failure deadline is measured from it.
	ChangedAt time.Time
	Image     ImageID
	Type      InstanceType
	// Key is a copy of the key pair's public key taken at RunInstance.
	// Deleting the key pair later does not change it. Nil means no key.
	Key   *AuthorizedKey
	Power Power
	// RebootEpoch counts accepted reboots. Rebooting is a level, not an
	// edge: the agent reboots once whenever the epoch it last applied is
	// behind this one, so concurrent reboot requests coalesce.
	RebootEpoch uint32
}

func (s InstanceSpec) Ref() Ref   { return s.ID.Ref() }
func (s InstanceSpec) Gen() int64 { return s.Generation }
func (InstanceSpec) isSpec()      {}

// AuthorizedKey is the public half of a key pair as injected into a machine.
type AuthorizedKey struct {
	Name      KeyName
	PublicKey PublicKey
}

// MachineState is the power state the agent last reported.
type MachineState uint8

const (
	// MachineUnknown means never observed.
	MachineUnknown MachineState = iota
	MachineAbsent
	MachineOff
	MachineOn
)

// InstanceStatus is what the reconciler last observed for one instance. Only
// the reconciler produces InstanceStatus values, and only
// store.StatusStore.CommitInstanceStatus persists them.
type InstanceStatus struct {
	Observed Observed
	Machine  MachineState
	// RebootEpoch is the epoch the agent last applied.
	RebootEpoch uint32
	Retry       Retry
	UpdatedAt   time.Time
}

// Instance is the merged read model: desired spec plus observed status.
type Instance struct {
	Spec   InstanceSpec
	Status InstanceStatus
}

// InstanceLifecycle is the state callers see. It is derived from Spec and
// Status on every read and never stored, so it cannot drift from them.
type InstanceLifecycle uint8

const (
	InstancePending InstanceLifecycle = iota + 1
	InstanceRunning
	InstanceStopping
	InstanceStopped
	InstanceShuttingDown
	InstanceTerminated
	InstanceError
)

// Lifecycle derives the caller-visible state from desired and observed state.
func (i Instance) Lifecycle() InstanceLifecycle {
	if i.Status.Observed.Parked(i.Spec.Generation) {
		return InstanceError
	}
	converged := i.Status.Observed.Generation == i.Spec.Generation
	switch i.Spec.Power {
	case PowerTerminated:
		if converged {
			return InstanceTerminated
		}
		return InstanceShuttingDown
	case PowerStopped:
		if converged {
			return InstanceStopped
		}
		return InstanceStopping
	}
	// A reboot in flight on a powered-on machine still reads as running.
	if converged || i.Status.Machine == MachineOn {
		return InstanceRunning
	}
	return InstancePending
}
