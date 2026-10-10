package compute

import (
	"crypto/sha256"
	"encoding/binary"
	"hash"
)

// Intent is a validated request to change desired state. Only the New*
// constructors in this package build Intents, so every Intent value passed
// validation. Plan is the only consumer.
type Intent interface {
	Kind() OperationKind
	encode(e *encoder)
	plan(v View, env Env) (Change, error)
}

type runInstance struct {
	image ImageID
	typ   InstanceType
	key   KeyName
}

// setPower is StartInstance, StopInstance, and TerminateInstance. The wanted
// power determines the kind.
type setPower struct {
	instance InstanceID
	want     Power
}

type rebootInstance struct{ instance InstanceID }

type registerImage struct {
	name ImageName
	url  ImageURL
}

type createImage struct {
	instance InstanceID
	name     ImageName
}

type deregisterImage struct{ image ImageID }

type createKeyPair struct{ name KeyName }

type importKeyPair struct {
	name KeyName
	key  PublicKey
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
	return newSetPower(instanceID, PowerRunning)
}

func NewStopInstance(instanceID string) (Intent, error) {
	return newSetPower(instanceID, PowerStopped)
}

func NewTerminateInstance(instanceID string) (Intent, error) {
	return newSetPower(instanceID, PowerTerminated)
}

func newSetPower(instanceID string, want Power) (Intent, error) {
	id, err := ParseInstanceID(instanceID)
	if err != nil {
		return nil, err
	}
	return setPower{instance: id, want: want}, nil
}

func NewRebootInstance(instanceID string) (Intent, error) {
	id, err := ParseInstanceID(instanceID)
	if err != nil {
		return nil, err
	}
	return rebootInstance{instance: id}, nil
}

// NewRegisterImage registers the disk image at sourceURL, which must be
// https.
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

// NewImportKeyPair takes one authorized_keys line.
func NewImportKeyPair(name, publicKey string) (Intent, error) {
	n, err := ParseKeyName(name)
	if err != nil {
		return nil, err
	}
	k, err := ParsePublicKey(publicKey)
	if err != nil {
		return nil, err
	}
	return importKeyPair{name: n, key: k}, nil
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
	}
	return OpTerminateInstance
}

func (rebootInstance) Kind() OperationKind  { return OpRebootInstance }
func (registerImage) Kind() OperationKind   { return OpRegisterImage }
func (createImage) Kind() OperationKind     { return OpCreateImage }
func (deregisterImage) Kind() OperationKind { return OpDeregisterImage }
func (createKeyPair) Kind() OperationKind   { return OpCreateKeyPair }
func (importKeyPair) Kind() OperationKind   { return OpImportKeyPair }
func (deleteKeyPair) Kind() OperationKind   { return OpDeleteKeyPair }

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

func (in setPower) encode(e *encoder)       { e.field(in.instance.s) }
func (in rebootInstance) encode(e *encoder) { e.field(in.instance.s) }

func (in registerImage) encode(e *encoder) {
	e.field(in.name.s)
	e.field(in.url.s)
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
}

func (in deleteKeyPair) encode(e *encoder) { e.field(in.name.s) }
