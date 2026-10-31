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
	// network. Like Image, it is fixed when the machine is created. It is
	// empty when Interfaces is set.
	Subnet string
	// Interfaces lists every network interface of a machine that has a
	// direct interface, in device index order. Empty means the machine has
	// at most the one overlay interface that Subnet names. Like Image, it is
	// fixed when the machine is created.
	Interfaces []Interface
}

// Interface is one network interface of a machine.
type Interface struct {
	// Name is the interface name inside the machine spec, a DNS-1123 label
	// unique per machine.
	Name string
	// Subnet is the Kube-OVN Subnet name of an overlay interface. Exactly
	// one of Subnet and Direct is set.
	Subnet string
	// Direct is the name of the direct (provider) network the interface
	// joins: the Kube-OVN Subnet and NetworkAttachmentDefinition are both
	// vws-direct-<Direct>.
	Direct string
	// Mode is the direct attachment mode. Empty or ModeBridge is an OVN
	// port bridged to the guest; any other mode is an extension point that
	// no agent implements yet.
	Mode string
	// IPv4 is the fixed address of a direct interface, or empty for one
	// that Kube-OVN assigns.
	IPv4 string
}

// ModeBridge is the default direct attachment mode.
const ModeBridge = "bridge"

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

// attachmentError is a permanent refusal to attach or detach one volume. The
// machine itself is healthy, so only the attachment operation fails.
type attachmentError struct {
	volume Volume
	err    error
}

func (e *attachmentError) Error() string { return e.err.Error() }
func (e *attachmentError) Unwrap() error { return e.err }

// Attachment marks err as a permanent failure to attach or detach volume. The
// machine itself is healthy, so callers fail the attachment operation and
// leave the instance running. A nil err stays nil.
func Attachment(volume Volume, err error) error {
	if err == nil {
		return nil
	}
	return Permanent(&causeError{cause: CauseAttachment, err: &attachmentError{volume: volume, err: err}})
}

// AttachmentVolume returns the volume an attachment error names.
func AttachmentVolume(err error) (Volume, bool) {
	var a *attachmentError
	if errors.As(err, &a) {
		return a.volume, true
	}
	return Volume{}, false
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

// VolumePhase is how far one data volume's hot-plug has got on a machine.
type VolumePhase uint8

const (
	// VolumePlugging means the volume is in the machine's spec and the
	// hypervisor has not finished plugging it into the guest.
	VolumePlugging VolumePhase = iota + 1
	// VolumePlugged means the hot-plug finished and the guest sees the disk.
	// A volume of a stopped machine is plugged: it is in the definition the
	// machine boots with.
	VolumePlugged
	// VolumeUnplugging means the volume left the machine's spec and the
	// hypervisor still has it on the running guest.
	VolumeUnplugging
)

// VolumeObserver is implemented by an agent that can say how far each data
// volume's hot-plug has got. A converged spec (ConvergeMachine returned nil)
// only means the hypervisor accepted the volume list; the guest may not see
// the disk yet. The reconciler reports a volume as attached only once it is
// VolumePlugged.
type VolumeObserver interface {
	// MachineVolumes returns the phase of every data volume the machine
	// lists or is still unplugging, by Volume.Name. A machine that does not
	// exist has none.
	MachineVolumes(ctx context.Context, name string) (map[string]VolumePhase, error)
}
