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
	Image          string
	VCPUs          int
	MemoryMiB      int
	AuthorizedKeys []string
	Power          Power
	// RebootEpoch is a level. A running machine whose last applied epoch is
	// behind it reboots once and records it. Booting a machine applies the
	// epoch without an extra reboot.
	RebootEpoch uint32
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
