package compute

import "time"

// ImageSource says where an image's bytes come from. FromURL and
// FromInstance are the only implementations.
type ImageSource interface{ isImageSource() }

// FromURL registers an existing disk image the agent fetches over https.
type FromURL struct{ URL ImageURL }

// FromInstance captures the root disk of an instance.
type FromInstance struct{ Instance InstanceID }

func (FromURL) isImageSource()      {}
func (FromInstance) isImageSource() {}

// ImageSpec is the desired state of one image at one generation.
type ImageSpec struct {
	ID         ImageID
	Generation int64
	CreatedAt  time.Time
	ChangedAt  time.Time
	Name       ImageName
	Source     ImageSource
	// Deregistered is absorbing. A deregistered image keeps its row as a
	// tombstone.
	Deregistered bool
}

func (s ImageSpec) Ref() Ref   { return s.ID.Ref() }
func (s ImageSpec) Gen() int64 { return s.Generation }
func (ImageSpec) isSpec()      {}

// ImageStatus is what the reconciler last observed for one image.
type ImageStatus struct {
	Observed Observed
	// Present reports that the artifact exists and is complete on the agent.
	Present   bool
	SizeBytes int64
	Retry     Retry
	UpdatedAt time.Time
}

// Image is the merged read model.
type Image struct {
	Spec   ImageSpec
	Status ImageStatus
}

// ImageLifecycle is the caller-visible state, derived on every read.
type ImageLifecycle uint8

const (
	ImagePending ImageLifecycle = iota + 1
	ImageAvailable
	ImageFailed
	ImageDeregistered
)

// Lifecycle derives the caller-visible state.
func (i Image) Lifecycle() ImageLifecycle {
	switch {
	case i.Spec.Deregistered:
		return ImageDeregistered
	case i.Status.Observed.Parked(i.Spec.Generation):
		return ImageFailed
	case i.Status.Observed.Generation == i.Spec.Generation && i.Status.Present:
		return ImageAvailable
	}
	return ImagePending
}

// Available reports whether instances may launch from i.
func (i Image) Available() bool { return i.Lifecycle() == ImageAvailable }
