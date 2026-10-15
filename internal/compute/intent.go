package compute

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"hash"
)

// Intent is a validated request to change desired state. Only the New*
// constructors in this package build Intents, so every Intent value passed
// validation. Plan is the only consumer.
type Intent interface {
	Kind() OperationKind
	encode(e *encoder)
	plan(v View, env Env) (Change, error)
	// carriesMetadata reports whether the request names labels or a
	// description. When such a request writes no spec generation, its
	// operation is MetadataOnly and Settle succeeds it at commit, so an
	// in-flight or parked spec generation cannot hold it Pending or fail it.
	carriesMetadata() bool
}

type runInstance struct {
	image ImageID
	typ   InstanceType
	key   KeyName
}

// setPower is StartInstance, StopInstance, and TerminateInstance. The wanted
// power determines the kind.
// setPower is StartInstance, StopInstance, and TerminateInstance, plus the
// power half of UpdateInstance. A non-zero patch is applied in the same
// commit as any power change. A zero patch encodes nothing, so Start, Stop,
// and Terminate keep the fingerprints they had before metadata existed.
type setPower struct {
	instance InstanceID
	want     Power
	patch    MetadataPatch
	// volumes, when setVolumes is true, replaces a controller-driven
	// instance's attachments: AttachVolume and DetachVolume through
	// UpdateInstance.
	setVolumes bool
	volumes    []VolumeAttachment
}

type rebootInstance struct{ instance InstanceID }

type registerImage struct {
	name ImageName
	url  ImageURL
}

type copyImage struct {
	id          ImageID
	scope       ImageScope
	source      CopySource
	family      ImageName
	description Description
	labels      Labels
}

type registerFile struct {
	id     ImageID
	path   ImagePath
	family ImageName
}

type createImage struct {
	instance InstanceID
	name     ImageName
}

type deregisterImage struct{ image ImageID }

type createKeyPair struct{ name KeyName }

type importKeyPair struct {
	name   KeyName
	key    PublicKey
	labels Labels
}

// The metadata intents change metadata only and never write a spec generation.
type updateInstanceMetadata struct {
	instance InstanceID
	patch    MetadataPatch
}

type updateImageMetadata struct {
	image ImageID
	patch MetadataPatch
}

// updateKeyPairMetadata names the key pair the way deleteKeyPair does: by
// name, resolved to the live incarnation at plan time.
type updateKeyPairMetadata struct {
	name  KeyName
	patch MetadataPatch
}

type deleteKeyPair struct{ name KeyName }

// NewRunInstance launches one instance. An empty keyName launches without an
// authorized key.
func NewRunInstance(imageID, instanceType, keyName string) (Intent, error) {
	img, err := ParseImageID(imageID)
	if err != nil {
		return nil, err
	}
	typ, err := ParseInstanceType(instanceType)
	if err != nil {
		return nil, err
	}
	var key KeyName
	if keyName != "" {
		if key, err = ParseKeyName(keyName); err != nil {
			return nil, err
		}
	}
	return runInstance{image: img, typ: typ, key: key}, nil
}

func NewStartInstance(instanceID string) (Intent, error) {
	return newSetPower(instanceID, PowerRunning, MetadataPatch{})
}

func NewStopInstance(instanceID string) (Intent, error) {
	return newSetPower(instanceID, PowerStopped, MetadataPatch{})
}

func NewTerminateInstance(instanceID string) (Intent, error) {
	return newSetPower(instanceID, PowerTerminated, MetadataPatch{})
}

// NewSetPower moves an instance to power and applies patch. power must be
// PowerRunning or PowerStopped. With a zero patch it is NewStartInstance or
// NewStopInstance, with the same kind and the same fingerprint. Terminate
// has its own constructor and does not carry a patch.
func NewSetPower(instanceID string, power Power, patch MetadataPatch) (Intent, error) {
	switch power {
	case PowerRunning, PowerStopped:
	default:
		return nil, Errorf(CodeInvalid, "power must be running or stopped")
	}
	return newSetPower(instanceID, power, patch)
}

// NewUpdateInstanceSpec is NewSetPower that also replaces the volume
// attachments of a controller-driven instance. The boot entry cannot change;
// data volumes are attached and detached on a running machine.
func NewUpdateInstanceSpec(instanceID string, power Power, volumes []VolumeAttachment, patch MetadataPatch) (Intent, error) {
	if power != PowerRunning && power != PowerStopped {
		return nil, Errorf(CodeInvalid, "power must be running or stopped")
	}
	id, err := ParseInstanceID(instanceID)
	if err != nil {
		return nil, err
	}
	return setPower{instance: id, want: power, patch: patch, setVolumes: true,
		volumes: append([]VolumeAttachment(nil), volumes...)}, nil
}

func newSetPower(instanceID string, want Power, patch MetadataPatch) (Intent, error) {
	id, err := ParseInstanceID(instanceID)
	if err != nil {
		return nil, err
	}
	return setPower{instance: id, want: want, patch: patch}, nil
}

func NewRebootInstance(instanceID string) (Intent, error) {
	id, err := ParseInstanceID(instanceID)
	if err != nil {
		return nil, err
	}
	return rebootInstance{instance: id}, nil
}

// NewCopyImage registers a copy of a volume or snapshot under id, the
// caller's image id. family may be empty. Plan fails with CodeAlreadyExists
// when any image, live or deregistered, already holds id.
func NewCopyImage(id string, scope ImageScope, source CopySource, family, description string, labels Labels) (Intent, error) {
	img, err := ParseImageID(id)
	if err != nil {
		return nil, err
	}
	if scope.IsZero() {
		return nil, Errorf(CodeInvalid, "image project and region are required")
	}
	if source == nil {
		return nil, Errorf(CodeInvalid, "image source is required")
	}
	var name ImageName
	if family != "" {
		if name, err = ParseImageName(family); err != nil {
			return nil, err
		}
	}
	desc, err := ParseDescription(description)
	if err != nil {
		return nil, err
	}
	return copyImage{id: img, scope: scope, source: source, family: name, description: desc, labels: labels}, nil
}

// NewRegisterFile registers the disk image file at path under id. path is a
// path on the agent's host. Plan fails with CodeAlreadyExists when id is taken.
func NewRegisterFile(id, path, family string) (Intent, error) {
	img, err := ParseImageID(id)
	if err != nil {
		return nil, err
	}
	p, err := ParseImagePath(path)
	if err != nil {
		return nil, err
	}
	name, err := ParseImageName(family)
	if err != nil {
		return nil, err
	}
	return registerFile{id: img, path: p, family: name}, nil
}

// NewRegisterImage registers the disk image at sourceURL, which must be
// https. Plan mints the image id.
func NewRegisterImage(name, sourceURL string) (Intent, error) {
	n, err := ParseImageName(name)
	if err != nil {
		return nil, err
	}
	u, err := ParseImageURL(sourceURL)
	if err != nil {
		return nil, err
	}
	return registerImage{name: n, url: u}, nil
}

// NewCreateImage captures the root disk of an instance.
func NewCreateImage(instanceID, name string) (Intent, error) {
	id, err := ParseInstanceID(instanceID)
	if err != nil {
		return nil, err
	}
	n, err := ParseImageName(name)
	if err != nil {
		return nil, err
	}
	return createImage{instance: id, name: n}, nil
}

func NewDeregisterImage(imageID string) (Intent, error) {
	id, err := ParseImageID(imageID)
	if err != nil {
		return nil, err
	}
	return deregisterImage{image: id}, nil
}

// NewCreateKeyPair generates an ed25519 key pair at plan time.
func NewCreateKeyPair(name string) (Intent, error) {
	n, err := ParseKeyName(name)
	if err != nil {
		return nil, err
	}
	return createKeyPair{name: n}, nil
}

// NewImportKeyPair takes one authorized_keys line and the key pair's initial
// labels. Empty labels leave the fingerprint exactly what it was before
// labels existed.
func NewImportKeyPair(name, publicKey string, labels Labels) (Intent, error) {
	n, err := ParseKeyName(name)
	if err != nil {
		return nil, err
	}
	k, err := ParsePublicKey(publicKey)
	if err != nil {
		return nil, err
	}
	return importKeyPair{name: n, key: k, labels: labels}, nil
}

// NewUpdateInstanceMetadata replaces metadata on a live instance and does
// not change its spec generation.
func NewUpdateInstanceMetadata(instanceID string, patch MetadataPatch) (Intent, error) {
	id, err := ParseInstanceID(instanceID)
	if err != nil {
		return nil, err
	}
	return updateInstanceMetadata{instance: id, patch: patch}, nil
}

// NewUpdateImageMetadata replaces metadata on an image that is not
// deregistered and does not change its spec generation.
func NewUpdateImageMetadata(imageID string, patch MetadataPatch) (Intent, error) {
	id, err := ParseImageID(imageID)
	if err != nil {
		return nil, err
	}
	return updateImageMetadata{image: id, patch: patch}, nil
}

// NewUpdateKeyPairMetadata replaces metadata on the live key pair with name.
func NewUpdateKeyPairMetadata(name string, patch MetadataPatch) (Intent, error) {
	n, err := ParseKeyName(name)
	if err != nil {
		return nil, err
	}
	return updateKeyPairMetadata{name: n, patch: patch}, nil
}

func NewDeleteKeyPair(name string) (Intent, error) {
	n, err := ParseKeyName(name)
	if err != nil {
		return nil, err
	}
	return deleteKeyPair{name: n}, nil
}

func (runInstance) Kind() OperationKind { return OpRunInstance }

func (in setPower) Kind() OperationKind {
	switch in.want {
	case PowerRunning:
		return OpStartInstance
	case PowerStopped:
		return OpStopInstance
	case PowerTerminated:
		return OpTerminateInstance
	default:
		return OpTerminateInstance
	}
}

func (rebootInstance) Kind() OperationKind         { return OpRebootInstance }
func (registerImage) Kind() OperationKind          { return OpRegisterImage }
func (copyImage) Kind() OperationKind              { return OpCopyImage }
func (registerFile) Kind() OperationKind           { return OpRegisterFile }
func (createImage) Kind() OperationKind            { return OpCreateImage }
func (deregisterImage) Kind() OperationKind        { return OpDeregisterImage }
func (createKeyPair) Kind() OperationKind          { return OpCreateKeyPair }
func (importKeyPair) Kind() OperationKind          { return OpImportKeyPair }
func (deleteKeyPair) Kind() OperationKind          { return OpDeleteKeyPair }
func (updateInstanceMetadata) Kind() OperationKind { return OpUpdateInstanceMetadata }
func (updateImageMetadata) Kind() OperationKind    { return OpUpdateImageMetadata }
func (updateKeyPairMetadata) Kind() OperationKind  { return OpUpdateKeyPairMetadata }

func (runInstance) carriesMetadata() bool    { return false }
func (in setPower) carriesMetadata() bool    { return !in.patch.IsZero() }
func (rebootInstance) carriesMetadata() bool { return false }
func (registerImage) carriesMetadata() bool  { return false }
func (in copyImage) carriesMetadata() bool {
	return in.description.String() != "" || in.labels.Len() > 0
}
func (registerFile) carriesMetadata() bool           { return false }
func (createImage) carriesMetadata() bool            { return false }
func (deregisterImage) carriesMetadata() bool        { return false }
func (createKeyPair) carriesMetadata() bool          { return false }
func (in importKeyPair) carriesMetadata() bool       { return in.labels.Len() > 0 }
func (deleteKeyPair) carriesMetadata() bool          { return false }
func (updateInstanceMetadata) carriesMetadata() bool { return true }
func (updateImageMetadata) carriesMetadata() bool    { return true }
func (updateKeyPairMetadata) carriesMetadata() bool  { return true }

// Fingerprint identifies an intent's content. Two intents have equal
// fingerprints iff they have the same Kind and equal fields.
type Fingerprint [32]byte

// FingerprintOf hashes in's canonical encoding: a version tag, the kind
// name, then each field length-prefixed in a fixed order per kind. Stores
// persist it, so the encoding must stay byte-stable across releases or
// in-flight retries fail with CodeRequestMismatch.
func FingerprintOf(in Intent) Fingerprint {
	e := encoder{h: sha256.New()}
	e.field("vws.compute.fingerprint.v1")
	e.field(in.Kind().String())
	in.encode(&e)
	var fp Fingerprint
	e.h.Sum(fp[:0])
	return fp
}

// LegacyFingerprintOf is the fingerprint a CreateImage intent had before
// image scopes (#92), which stores written by an older release persist, with
// the scope the intent now carries. ok is false for every other intent,
// whose encoding did not change. A replay may accept the legacy fingerprint
// only after checking that the stored image has this scope.
func LegacyFingerprintOf(in Intent) (fp Fingerprint, scope ImageScope, ok bool) {
	c, isCopy := in.(copyImage)
	if !isCopy {
		return Fingerprint{}, ImageScope{}, false
	}
	e := encoder{h: sha256.New()}
	e.field("vws.compute.fingerprint.v1")
	e.field(in.Kind().String())
	c.encodeScoped(&e, false)
	e.h.Sum(fp[:0])
	return fp, c.scope, true
}

type encoder struct{ h hash.Hash }

func (e *encoder) field(s string) {
	var n [4]byte
	binary.BigEndian.PutUint32(n[:], uint32(len(s)))
	e.h.Write(n[:])
	e.h.Write([]byte(s))
}

func (in runInstance) encode(e *encoder) {
	e.field(in.image.s)
	e.field(in.typ.name)
	e.field(in.key.s)
}

// encode adds the volumes only when the intent sets them, so start, stop,
// and terminate keep their persisted fingerprints.
func (in setPower) encode(e *encoder) {
	e.field(in.instance.s)
	in.patch.encode(e)
	if in.setVolumes {
		e.field("volumes=")
		e.field(fmt.Sprint(len(in.volumes)))
		for _, a := range in.volumes {
			e.field(a.Volume.s)
			e.field(a.DeviceName)
			e.field(fmt.Sprint(a.Boot))
		}
	}
}
func (in rebootInstance) encode(e *encoder) { e.field(in.instance.s) }

func (in registerImage) encode(e *encoder) {
	e.field(in.name.s)
	e.field(in.url.s)
}

func (in copyImage) encode(e *encoder) { in.encodeScoped(e, true) }

// encodeScoped writes the fields; withScope false is the encoding before
// image scopes (#92), which LegacyFingerprintOf reproduces.
func (in copyImage) encodeScoped(e *encoder, withScope bool) {
	e.field(in.id.s)
	if withScope {
		e.field(in.scope.project)
		e.field(in.scope.region)
	}
	switch src := in.source.(type) {
	case FromVolume:
		e.field("volume")
		e.field(src.Ref.String())
	case FromSnapshot:
		e.field("snapshot")
		e.field(src.Ref.String())
	default:
		e.field("unknown")
		e.field("")
	}
	e.field(in.family.s)
	e.field(in.description.s)
	if in.labels.Len() > 0 {
		e.labelSet("labels=", in.labels)
	}
}

func (in registerFile) encode(e *encoder) {
	e.field(in.id.s)
	e.field(in.path.s)
	e.field(in.family.s)
}

func (in createImage) encode(e *encoder) {
	e.field(in.instance.s)
	e.field(in.name.s)
}

func (in deregisterImage) encode(e *encoder) { e.field(in.image.s) }
func (in createKeyPair) encode(e *encoder)   { e.field(in.name.s) }

func (in importKeyPair) encode(e *encoder) {
	e.field(in.name.s)
	e.field(in.key.wire)
	if in.labels.Len() > 0 {
		e.labelSet("labels=", in.labels)
	}
}

func (in deleteKeyPair) encode(e *encoder) { e.field(in.name.s) }

func (in updateInstanceMetadata) encode(e *encoder) {
	e.field(in.instance.s)
	in.patch.encode(e)
}

func (in updateImageMetadata) encode(e *encoder) {
	e.field(in.image.s)
	in.patch.encode(e)
}

func (in updateKeyPairMetadata) encode(e *encoder) {
	e.field(in.name.s)
	in.patch.encode(e)
}
