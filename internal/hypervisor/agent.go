// Package hypervisor is the contract between the reconciler and whatever runs
// machines and stores disk images.
//
// It imports nothing from this module. Its types are plain data: machines are
// named by instance ID and images by image ID, so an implementation never
// sees control-plane types. Only internal/reconciler calls an Agent.
package hypervisor

import (
	"context"
	"errors"
)

// Power is a machine's power state as the agent applies it.
type Power uint8

const (
	// PowerAbsent means the machine must not exist. Converging to it
	// destroys the machine and its disk.
	PowerAbsent Power = iota + 1
	PowerOff
	PowerOn
)

// MachineSpec is the full desired state of one machine. The agent converges
// to it from whatever state the machine is in, so the same spec may be
// applied any number of times.
type MachineSpec struct {
	// Name is the instance ID.
	Name string
	// Image is the image ID to create the root disk from. It is read only
	// when the machine does not exist yet.
	Image string
	// BootClaim is the existing PersistentVolumeClaim to boot from when
	// Image is empty: a controller-driven instance's boot volume. Like
	// Image, it is fixed when the machine is created.
	BootClaim      string
	VCPUs          int
	MemoryMiB      int
	AuthorizedKeys []string
	// UserData is the instance's user data, opaque text for the guest's
	// cloud-init. Like AuthorizedKeys, it is read only when the machine is
	// created.
	UserData string
	Power    Power
	// RebootEpoch is a level. A running machine whose last applied epoch is
	// behind it reboots once and records it. Booting a machine applies the
	// epoch without an extra reboot.
	RebootEpoch uint32
	// Volumes is the full set of data volumes attached to the machine, on top
	// of its root disk. The agent attaches the ones that are missing and
	// detaches the ones that are no longer listed, on a running machine too.
	Volumes []Volume
	// Subnet is the network the machine's first interface joins: the VWS
	// subnet ID, which is also the name of the Kube-OVN Subnet that
	// vws-network creates for it. Empty means the cluster's default pod
	// network. Like Image, it is fixed when the machine is created.
	Subnet string
}

// Volume is one attached data volume.
type Volume struct {
	// Name is the disk name inside the machine. It is unique per machine and
	// is a DNS-1123 label.
	Name string
	// Claim is the name of the PersistentVolumeClaim that backs the volume.
	Claim string
}

// ImageSpec is the full desired state of one image artifact.
type ImageSpec struct {
	// Name is the image ID.
	Name string
	// Present false means the artifact must not exist.
	Present bool
	// Exactly one source is set. SourceMachine is the instance ID whose root
	// disk is captured. SourcePath is a disk image file on the agent's host.
	// SourceVolume and SourceSnapshot are resource names of a copy.
	SourceURL      string
	SourcePath     string
	SourceMachine  string
	SourceVolume   string
	SourceSnapshot string
}

// ImageAuth is who may clone an image artifact: the project and region that
// own it, and with Public every project in Region. The zero value lets
// nobody clone it.
type ImageAuth struct {
	Project string
	Region  string
	Public  bool
}

// ImageAuthorizer is an Agent whose image artifacts carry their clone
// authorization (the kubevirt agent's DataSource labels). AuthorizeImages
// brings every image artifact the agent manages, whatever its state, to
// auth(name) and is a no-op when they already match. An artifact auth does
// not know gets the zero ImageAuth.
type ImageAuthorizer interface {
	AuthorizeImages(ctx context.Context, auth func(name string) ImageAuth) error
}

// Agent converges machines and images. A nil error means the spec is fully
// applied. An error wrapped by Permanent means retrying the same spec cannot
// succeed. Any other error is transient and the caller retries the same spec
// later.
type Agent interface {
	ConvergeMachine(ctx context.Context, spec MachineSpec) error
	// ConvergeImage returns the artifact's size in bytes, which is 0 when
	// spec.Present is false.
	ConvergeImage(ctx context.Context, spec ImageSpec) (sizeBytes int64, err error)
}

// PermanentError marks an agent error that retrying cannot fix.
type PermanentError struct{ Err error }

func (e *PermanentError) Error() string { return e.Err.Error() }
func (e *PermanentError) Unwrap() error { return e.Err }

// Permanent wraps err as a PermanentError. It returns nil for nil.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &PermanentError{Err: err}
}

// IsPermanent reports whether err's chain holds a PermanentError.
func IsPermanent(err error) bool {
	var p *PermanentError
	return errors.As(err, &p)
}

// Cause is why an agent error cannot succeed on retry.
type Cause uint8

const (
	CauseNone Cause = iota
	CauseImageNotFound
	// CauseAttachment is a volume the machine cannot attach or detach. The
	// machine is healthy, so only the attachment operation fails.
	CauseAttachment
)

type causeError struct {
	cause Cause
	err   error
}

func (e *causeError) Error() string { return e.err.Error() }
func (e *causeError) Unwrap() error { return e.err }

// ImageNotFound marks err as a permanent failure because the image does not
// exist. A nil err stays nil.
func ImageNotFound(err error) error {
	if err == nil {
		return nil
	}
	return Permanent(&causeError{cause: CauseImageNotFound, err: err})
}

// Attachment marks err as a permanent failure to attach or detach a volume.
// The machine itself is healthy, so callers fail the attachment operation
// and leave the instance running. A nil err stays nil.
func Attachment(err error) error {
	if err == nil {
		return nil
	}
	return Permanent(&causeError{cause: CauseAttachment, err: err})
}

// CauseOf walks err's chain. Permanent and NotReady do not hide a cause
// they wrap. An error with no cause is CauseNone.
func CauseOf(err error) Cause {
	var c *causeError
	if errors.As(err, &c) {
		return c.cause
	}
	return CauseNone
}

// NotReadyError marks a transient agent error that only says the machine is
// still coming up: the spec is applied and nothing is wrong, the hypervisor
// has not finished yet. Callers retry it like any transient error but need
// not log it as a failure.
type NotReadyError struct{ Err error }

func (e *NotReadyError) Error() string { return e.Err.Error() }
func (e *NotReadyError) Unwrap() error { return e.Err }

// NotReady wraps err as a NotReadyError. It returns nil for nil.
func NotReady(err error) error {
	if err == nil {
		return nil
	}
	return &NotReadyError{Err: err}
}

// IsNotReady reports whether err's chain holds a NotReadyError.
func IsNotReady(err error) bool {
	var n *NotReadyError
	return errors.As(err, &n)
}

// AddressObserver is implemented by an agent that can say which private
// IPv4 address a machine's first interface (the one joined to
// MachineSpec.Subnet) has. The reconciler reports it as the instance's
// observed interface.
type AddressObserver interface {
	// MachineIPv4 returns the address, or "" while the machine has none
	// yet or does not exist.
	MachineIPv4(ctx context.Context, name string) (string, error)
}
