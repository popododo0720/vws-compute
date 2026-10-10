package compute

import "time"

// ImageSource says where an image's bytes come from. FromURL, FromInstance,
// FromFile, FromVolume, and FromSnapshot are the only implementations.
type ImageSource interface{ isImageSource() }

// CopySource is the subset CreateImage may store: a volume or a snapshot
// resource name. A file path does not implement it.
type CopySource interface {
	ImageSource
	isCopySource()
}

// FromURL registers an existing disk image the agent fetches over https.
type FromURL struct{ URL ImageURL }

// FromInstance captures the root disk of an instance.
type FromInstance struct{ Instance InstanceID }

// FromFile registers a disk image file on the agent's host. The seed loader
// is the only writer. CreateImage cannot name a file.
type FromFile struct{ Path ImagePath }

// FromVolume records a copy of a volume resource. This process does not host
// the volume; it stores the name and the agent records the copy.
type FromVolume struct{ Ref VolumeRef }

// FromSnapshot records a copy of a snapshot resource.
type FromSnapshot struct{ Ref SnapshotRef }

func (FromURL) isImageSource()      {}
func (FromInstance) isImageSource() {}
func (FromFile) isImageSource()     {}
func (FromVolume) isImageSource()   {}
func (FromSnapshot) isImageSource() {}

func (FromVolume) isCopySource()   {}
func (FromSnapshot) isCopySource() {}

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
	Meta   Metadata
}

// ImageLifecycle is the caller-visible state, derived on every read.
type ImageLifecycle uint8

const (
	ImagePending ImageLifecycle = iota + 1
	ImageAvailable
	ImageFailed
	ImageDeregistered
)

// Lifecycle derives the caller-visible state. A parked generation is Failed
// even when the desired spec is already deregistered, so a capture or a
// delete that gave up is not reported as done.
func (i Image) Lifecycle() ImageLifecycle {
	switch {
	case i.Status.Observed.Parked(i.Spec.Generation):
		return ImageFailed
	case i.Spec.Deregistered && i.Status.Observed.Generation == i.Spec.Generation:
		return ImageDeregistered
	case !i.Spec.Deregistered && i.Status.Observed.Generation == i.Spec.Generation && i.Status.Present:
		return ImageAvailable
	}
	return ImagePending
}

// Available reports whether instances may launch from i.
func (i Image) Available() bool { return i.Lifecycle() == ImageAvailable }
