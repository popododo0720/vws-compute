package compute

import (
	"io"
	"time"
)

// View is the read-only slice of state a planner may consult. Missing
// resources fail with CodeNotFound. The controller's implementation records
// the generation of every resource it returns, and CommitDesired re-checks
// them atomically, so a plan never commits against state that changed
// underneath it.
type View interface {
	Instance(InstanceID) (Instance, error)
	Image(ImageID) (Image, error)
	// KeyPairByName returns the live key pair holding name, or, when none
	// is live, the most recently deleted one.
	KeyPairByName(KeyName) (KeyPair, error)
}

// Env is everything nondeterministic a planner needs. It is injected, so
// planners are pure functions of (View, Env, Intent).
type Env struct {
	Now  time.Time
	Rand io.Reader
}

// Change is a planned desired-state change to exactly one resource, which is
// what lets one Operation track one (resource, generation) pair.
type Change struct {
	// Spec is the target's desired state after the intent. Spec.Ref() and
	// Spec.Gen() become the operation's Target and TargetGeneration.
	Spec Spec
	// Write is false when the stored spec already satisfies the intent, for
	// example StopInstance on a stopped instance. The operation then tracks
	// the existing generation, and Settle may settle it at birth.
	Write bool
	// Secret is the private key for CreateKeyPair and nil otherwise.
	// store.DesiredChange has no field for it.
	Secret *PrivateKey
}

// Plan applies in to the state visible through v. It is the only place the
// transition rules live.
func Plan(v View, env Env, in Intent) (Change, error) {
	if in == nil {
		return Change{}, Errorf(CodeInvalid, "no intent")
	}
	return in.plan(v, env)
}

// plan requires an available image and, when a key is named, a live key pair
// whose public key it copies into the new spec.
func (in runInstance) plan(v View, env Env) (Change, error) {
	img, err := v.Image(in.image)
	if err != nil {
		return Change{}, err
	}
	if !img.Available() {
		return Change{}, Errorf(CodeFailedPrecondition, "image %s is not available", in.image)
	}
	var key *AuthorizedKey
	if !in.key.IsZero() {
		kp, err := v.KeyPairByName(in.key)
		if err != nil {
			return Change{}, err
		}
		if kp.Spec.Deleted {
			return Change{}, Errorf(CodeNotFound, "key pair %q not found", in.key)
		}
		key = &AuthorizedKey{Name: kp.Spec.Name, PublicKey: kp.Spec.PublicKey}
	}
	id, err := newInstanceID(env.Rand)
	if err != nil {
		return Change{}, err
	}
	return Change{Write: true, Spec: InstanceSpec{
		ID:         id,
		Generation: 1,
		CreatedAt:  env.Now,
		ChangedAt:  env.Now,
		Image:      in.image,
		Type:       in.typ,
		Key:        key,
		Power:      PowerRunning,
	}}, nil
}

// plan moves an instance's desired power to in.want:
//
//	from \ want   Running        Stopped        Terminated
//	Running       no-op          write          write
//	Stopped       write          no-op          write
//	Terminated    FailedPrecond  FailedPrecond  no-op
//
// A no-op becomes a write when the current generation is parked: asking
// again for the desired state that failed is how a caller retries it.
func (in setPower) plan(v View, env Env) (Change, error) {
	inst, err := v.Instance(in.instance)
	if err != nil {
		return Change{}, err
	}
	cur := inst.Spec
	if cur.Power == PowerTerminated && in.want != PowerTerminated {
		return Change{}, Errorf(CodeFailedPrecondition, "instance %s is terminated", in.instance)
	}
	if cur.Power == in.want && !inst.Status.Observed.Parked(cur.Generation) {
		return Change{Spec: cur}, nil
	}
	next := cur
	next.Power = in.want
	next.Generation++
	next.ChangedAt = env.Now
	return Change{Spec: next, Write: true}, nil
}

// plan requires desired power Running. A reboot that is still unapplied
// (spec epoch ahead of the applied epoch) absorbs this one; otherwise the
// epoch and generation advance.
func (in rebootInstance) plan(v View, env Env) (Change, error) {
	inst, err := v.Instance(in.instance)
	if err != nil {
		return Change{}, err
	}
	cur := inst.Spec
	switch cur.Power {
	case PowerTerminated:
		return Change{}, Errorf(CodeFailedPrecondition, "instance %s is terminated", in.instance)
	case PowerStopped:
		return Change{}, Errorf(CodeFailedPrecondition, "instance %s is stopped", in.instance)
	}
	if !inst.Status.Observed.Parked(cur.Generation) && cur.RebootEpoch > inst.Status.RebootEpoch {
		return Change{Spec: cur}, nil
	}
	next := cur
	next.RebootEpoch++
	next.Generation++
	next.ChangedAt = env.Now
	return Change{Spec: next, Write: true}, nil
}

// plan reads nothing: a URL image depends on no other resource.
func (in registerImage) plan(_ View, env Env) (Change, error) {
	id, err := newImageID(env.Rand)
	if err != nil {
		return Change{}, err
	}
	return Change{Write: true, Spec: ImageSpec{
		ID:         id,
		Generation: 1,
		CreatedAt:  env.Now,
		ChangedAt:  env.Now,
		Name:       in.name,
		Source:     FromURL{URL: in.url},
	}}, nil
}

// plan requires the source instance to exist and not be desired Terminated.
// The agent captures the disk when it converges the image; if the machine is
// gone by then, the image generation fails and parks.
func (in createImage) plan(v View, env Env) (Change, error) {
	inst, err := v.Instance(in.instance)
	if err != nil {
		return Change{}, err
	}
	if inst.Spec.Power == PowerTerminated {
		return Change{}, Errorf(CodeFailedPrecondition, "instance %s is terminated", in.instance)
	}
	id, err := newImageID(env.Rand)
	if err != nil {
		return Change{}, err
	}
	return Change{Write: true, Spec: ImageSpec{
		ID:         id,
		Generation: 1,
		CreatedAt:  env.Now,
		ChangedAt:  env.Now,
		Name:       in.name,
		Source:     FromInstance{Instance: in.instance},
	}}, nil
}

// plan is a no-op on an already deregistered image unless that generation
// is parked.
func (in deregisterImage) plan(v View, env Env) (Change, error) {
	img, err := v.Image(in.image)
	if err != nil {
		return Change{}, err
	}
	cur := img.Spec
	if cur.Deregistered && !img.Status.Observed.Parked(cur.Generation) {
		return Change{Spec: cur}, nil
	}
	next := cur
	next.Deregistered = true
	next.Generation++
	next.ChangedAt = env.Now
	return Change{Spec: next, Write: true}, nil
}

// plan fails with CodeAlreadyExists if the name is live. The private key
// leaves Plan only in Change.Secret.
func (in createKeyPair) plan(v View, env Env) (Change, error) {
	if err := requireFreeName(v, in.name); err != nil {
		return Change{}, err
	}
	pub, priv, err := generateEd25519(env.Rand)
	if err != nil {
		return Change{}, err
	}
	ch, err := newKeyPair(env, in.name, pub)
	if err != nil {
		return Change{}, err
	}
	ch.Secret = &priv
	return ch, nil
}

// plan fails with CodeAlreadyExists if the name is live.
func (in importKeyPair) plan(v View, env Env) (Change, error) {
	if err := requireFreeName(v, in.name); err != nil {
		return Change{}, err
	}
	return newKeyPair(env, in.name, in.key)
}

func requireFreeName(v View, name KeyName) error {
	kp, err := v.KeyPairByName(name)
	switch {
	case err == nil && !kp.Spec.Deleted:
		return Errorf(CodeAlreadyExists, "key pair %q already exists", name)
	case err != nil && CodeOf(err) != CodeNotFound:
		return err
	}
	return nil
}

func newKeyPair(env Env, name KeyName, pub PublicKey) (Change, error) {
	id, err := newKeyPairID(env.Rand)
	if err != nil {
		return Change{}, err
	}
	return Change{Write: true, Spec: KeyPairSpec{
		ID:         id,
		Generation: 1,
		CreatedAt:  env.Now,
		ChangedAt:  env.Now,
		Name:       name,
		PublicKey:  pub,
	}}, nil
}

// plan fails with CodeNotFound if the name was never stored. Deleting a
// tombstone is a no-op on it, so a second delete with a new request id
// succeeds without writing.
func (in deleteKeyPair) plan(v View, env Env) (Change, error) {
	kp, err := v.KeyPairByName(in.name)
	if err != nil {
		return Change{}, err
	}
	cur := kp.Spec
	if cur.Deleted {
		return Change{Spec: cur}, nil
	}
	next := cur
	next.Deleted = true
	next.Generation++
	next.ChangedAt = env.Now
	return Change{Spec: next, Write: true}, nil
}
