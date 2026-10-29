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
	// Meta is the target's metadata after the intent, when the intent
	// changes it, and nil otherwise. A create sets it with Revision 0.
	// An update sets Revision to stored+1 through MetadataPatch.next.
	// Meta never makes Write true: metadata moves no generation.
	Meta *Metadata
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
	meta, err := in.patch.next(inst.Meta, env)
	if err != nil {
		return Change{}, err
	}
	volumes, changed, err := in.nextVolumes(cur)
	if err != nil {
		return Change{}, err
	}
	if cur.Power == in.want && !changed && !inst.Status.Observed.Parked(cur.Generation) {
		return Change{Spec: cur, Meta: meta}, nil
	}
	next := cur
	next.Power = in.want
	next.Volumes = withoutRefused(volumes, cur, inst.Status.Observed)
	next.Generation++
	next.ChangedAt = env.Now
	return Change{Spec: next, Write: true, Meta: meta}, nil
}

// withoutRefused drops the attachment whose attach the agent permanently
// refused. It must not stay in desired state, where it would make every later
// converge fail before a power change lands. EC2 leaves no attachment behind
// when AttachVolume fails.
//
// Only a stale carry-over is dropped: the failure must belong to cur's
// generation, and the entry must be in cur unchanged. An attachment the
// caller adds or changes in this intent is kept, so its operation either
// attaches the disk or fails; it never succeeds with the disk dropped.
func withoutRefused(vols []VolumeAttachment, cur InstanceSpec, obs Observed) []VolumeAttachment {
	refused := obs.AttachmentFailure
	if refused == nil || refused.Volume.String() == "" || obs.Generation != cur.Generation {
		return vols
	}
	kept := make([]VolumeAttachment, 0, len(vols))
	for _, v := range vols {
		if v.Volume == refused.Volume && hasAttachment(cur.Volumes, v) {
			continue
		}
		kept = append(kept, v)
	}
	return kept
}

// nextVolumes returns the attachments after in and whether they differ from
// cur's. Only a controller-driven, non-terminated instance takes volumes,
// and its boot entry is fixed at create.
func (in setPower) nextVolumes(cur InstanceSpec) ([]VolumeAttachment, bool, error) {
	if !in.setVolumes {
		return cur.Volumes, false, nil
	}
	if !cur.ControllerDriven() {
		return nil, false, Errorf(CodeFailedPrecondition, "instance %s has no volumes to change", in.instance)
	}
	if err := checkAttachments(cur.Zone, in.volumes); err != nil {
		return nil, false, err
	}
	for _, a := range in.volumes {
		if a.Boot && !hasAttachment(cur.Volumes, a) {
			return nil, false, Errorf(CodeInvalid, "the boot volume cannot be changed")
		}
	}
	if len(in.volumes) == len(cur.Volumes) {
		same := true
		for i := range in.volumes {
			same = same && sameAttachment(in.volumes[i], cur.Volumes[i])
		}
		if same {
			return cur.Volumes, false, nil
		}
	}
	if cur.Power == PowerTerminated {
		return nil, false, Errorf(CodeFailedPrecondition, "instance %s is terminated", in.instance)
	}
	return append([]VolumeAttachment(nil), in.volumes...), true, nil
}

func hasAttachment(vols []VolumeAttachment, a VolumeAttachment) bool {
	for _, v := range vols {
		if sameAttachment(v, a) {
			return true
		}
	}
	return false
}

// plan requires desired power Running. Every accepted reboot advances the
// epoch and the generation. The agent still reboots once per sweep, however
// far the epoch is behind, so two reboots accepted before it runs become one
// reboot and the earlier operation is superseded.
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
	next := cur
	next.Volumes = withoutRefused(cur.Volumes, cur, inst.Status.Observed)
	next.RebootEpoch++
	next.Generation++
	next.ChangedAt = env.Now
	return Change{Spec: next, Write: true}, nil
}

// plan refuses an id that any image already holds, including a tombstone,
// and writes generation 1. Two callers of the same id race at commit: the
// loser gets a conflict, replans, and then sees the winner.
func admitNewImage(v View, env Env, id ImageID, scope ImageScope, family ImageName, src ImageSource, meta *Metadata) (Change, error) {
	_, err := v.Image(id)
	switch {
	case err == nil:
		return Change{}, Errorf(CodeAlreadyExists, "image %s already exists", id)
	case CodeOf(err) != CodeNotFound:
		return Change{}, err
	}
	return Change{Write: true, Meta: meta, Spec: ImageSpec{
		ID:         id,
		Scope:      scope,
		Generation: 1,
		CreatedAt:  env.Now,
		ChangedAt:  env.Now,
		Name:       family,
		Source:     src,
	}}, nil
}

// plan reads nothing. The caller's id is not the stored image id, so a
// lookup would tell another project whether that id is taken.
func (in copyImage) plan(_ View, env Env) (Change, error) {
	id, err := newImageID(env.Rand)
	if err != nil {
		return Change{}, err
	}
	var meta *Metadata
	if in.description.String() != "" || in.labels.Len() > 0 {
		meta = &Metadata{Description: in.description, Labels: in.labels}
	}
	return Change{Write: true, Meta: meta, Spec: ImageSpec{
		ID:         id,
		Scope:      in.scope,
		Generation: 1,
		CreatedAt:  env.Now,
		ChangedAt:  env.Now,
		Name:       in.family,
		Source:     in.source,
	}}, nil
}

func (in registerFile) plan(v View, env Env) (Change, error) {
	return admitNewImage(v, env, in.id, ImageScope{}, in.family, FromFile{Path: in.path}, nil)
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
	ch, err := newKeyPair(env, in.name, in.key)
	if err != nil {
		return Change{}, err
	}
	if in.labels.Len() > 0 {
		ch.Meta = &Metadata{Labels: in.labels}
	}
	return ch, nil
}

func (in updateInstanceMetadata) plan(v View, env Env) (Change, error) {
	inst, err := v.Instance(in.instance)
	if err != nil {
		return Change{}, err
	}
	if inst.Spec.Power == PowerTerminated {
		return Change{}, Errorf(CodeFailedPrecondition, "instance %s is terminated", in.instance)
	}
	return metadataChange(inst.Spec, inst.Meta, in.patch, env)
}

func (in updateImageMetadata) plan(v View, env Env) (Change, error) {
	img, err := v.Image(in.image)
	if err != nil {
		return Change{}, err
	}
	if img.Spec.Deregistered {
		return Change{}, Errorf(CodeFailedPrecondition, "image %s is deregistered", in.image)
	}
	return metadataChange(img.Spec, img.Meta, in.patch, env)
}

func (in updateKeyPairMetadata) plan(v View, env Env) (Change, error) {
	kp, err := v.KeyPairByName(in.name)
	if err != nil {
		return Change{}, err
	}
	if kp.Spec.Deleted {
		return Change{}, Errorf(CodeNotFound, "key pair %q not found", in.name)
	}
	return metadataChange(kp.Spec, kp.Meta, in.patch, env)
}

func metadataChange(spec Spec, cur Metadata, p MetadataPatch, env Env) (Change, error) {
	meta, err := p.next(cur, env)
	if err != nil {
		return Change{}, err
	}
	return Change{Spec: spec, Meta: meta}, nil
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
