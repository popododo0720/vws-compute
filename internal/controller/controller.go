// Package controller admits intents. Submit is the only path that changes
// desired state, and this is the only package that calls
// store.DesiredStore.CommitDesired. It never talks to a hypervisor: a
// submitted operation is converged later by the reconciler.
package controller

import (
	"context"
	"crypto/rand"
	"errors"
	"io"
	"time"

	"github.com/popododo0720/vws-compute/internal/compute"
	"github.com/popododo0720/vws-compute/internal/store"
)

// MaxReplans is how many times Submit replans after losing an optimistic
// concurrency race before it fails with compute.CodeAborted.
const MaxReplans = 3

type Controller struct {
	store store.DesiredStore
	now   func() time.Time
	rand  io.Reader
	kick  func()
}

type Option func(*Controller)

// WithClock sets the time source for operation and spec timestamps.
func WithClock(now func() time.Time) Option { return func(c *Controller) { c.now = now } }

// WithRand sets the entropy source for IDs and generated keys.
func WithRand(r io.Reader) Option { return func(c *Controller) { c.rand = r } }

// WithKick sets a function Submit calls after committing work the reconciler
// must converge. It must not block. Without it the reconciler finds the
// work on its next periodic sweep.
func WithKick(kick func()) Option { return func(c *Controller) { c.kick = kick } }

func New(s store.DesiredStore, opts ...Option) *Controller {
	c := &Controller{store: s, now: time.Now, rand: rand.Reader, kick: func() {}}
	for _, o := range opts {
		o(c)
	}
	return c
}

// Receipt is Submit's result.
type Receipt struct {
	Operation compute.Operation
	// PrivateKey is set only for CreateKeyPair, and only on the call whose
	// operation was inserted. A replay of the same request returns nil: the
	// key was never stored.
	PrivateKey *compute.PrivateKey
	// KeyPair is set on that same inserting call, so the handler can return
	// the key without a second read. A failed read must not drop PrivateKey.
	KeyPair *compute.KeyPair
}

// Submit admits in under request id req.
//
// If req was already used with the same intent, Submit returns the stored
// operation and writes nothing. If it was used with a different intent, it
// fails with compute.CodeRequestMismatch. Otherwise it plans against current
// state and commits the plan with every version the plan read as a
// precondition, replanning up to MaxReplans times when that state changed
// underneath it.
func (c *Controller) Submit(ctx context.Context, req compute.RequestID, in compute.Intent) (Receipt, error) {
	if req.IsZero() || in == nil {
		return Receipt{}, compute.Errorf(compute.CodeInvalid, "a request id and an intent are required")
	}
	if r, ok, err := c.replay(ctx, req, in); ok || err != nil {
		return r, err
	}
	for attempt := 0; attempt <= MaxReplans; attempt++ {
		v := &recordingView{ctx: ctx, r: c.store}
		env := compute.Env{Now: c.now(), Rand: c.rand}
		ch, err := compute.Plan(v, env, in)
		if err != nil {
			// A concurrent call with the same request may have committed
			// first and changed what this plan saw, as a second
			// CreateKeyPair sees its own name taken.
			if r, ok, rerr := c.replay(ctx, req, in); ok || rerr != nil {
				return r, rerr
			}
			return Receipt{}, err
		}
		op, err := compute.NewOperation(env, req, in, ch)
		if err != nil {
			return Receipt{}, err
		}
		got, err := c.store.CommitDesired(ctx, store.DesiredChange{Op: op, Spec: ch.Spec, Write: ch.Write, Expect: v.read})
		if errors.Is(err, store.ErrConflict) {
			continue
		}
		if err != nil {
			return Receipt{}, err
		}
		if got.ID != op.ID {
			return Receipt{Operation: got}, nil
		}
		if !got.State().Terminal() {
			c.kick()
		}
		rc := Receipt{Operation: got, PrivateKey: ch.Secret}
		if spec, ok := ch.Spec.(compute.KeyPairSpec); ok && ch.Secret != nil {
			rc.KeyPair = &compute.KeyPair{Spec: spec}
		}
		return rc, nil
	}
	return Receipt{}, compute.Errorf(compute.CodeAborted,
		"request %s lost %d races with concurrent changes; retry with the same request id", req, MaxReplans+1)
}

// replay returns the stored operation for req, if there is one.
func (c *Controller) replay(ctx context.Context, req compute.RequestID, in compute.Intent) (Receipt, bool, error) {
	op, err := c.store.OperationByRequest(ctx, req)
	switch {
	case compute.CodeOf(err) == compute.CodeNotFound:
		return Receipt{}, false, nil
	case err != nil:
		return Receipt{}, false, err
	case op.Fingerprint != compute.FingerprintOf(in):
		return Receipt{}, false, compute.Errorf(compute.CodeRequestMismatch, "request id %s was used for a different request", req)
	}
	return Receipt{Operation: op}, true, nil
}

// recordingView is the compute.View a plan reads through. It records the
// spec generation, observed generation, and parked bit of every resource it
// returns so CommitDesired can reject the plan if any of them moved.
type recordingView struct {
	ctx  context.Context
	r    store.Reader
	read []store.Version
}

func (v *recordingView) record(ref compute.Ref, specGen int64, obs compute.Observed) {
	v.read = append(v.read, store.Version{
		Ref:        ref,
		Generation: specGen,
		Observed:   obs.Generation,
		Parked:     obs.Parked(specGen),
	})
}

func (v *recordingView) Instance(id compute.InstanceID) (compute.Instance, error) {
	inst, err := v.r.Instance(v.ctx, id)
	if err == nil {
		v.record(inst.Spec.Ref(), inst.Spec.Generation, inst.Status.Observed)
	}
	return inst, err
}

func (v *recordingView) Image(id compute.ImageID) (compute.Image, error) {
	img, err := v.r.Image(v.ctx, id)
	if err == nil {
		v.record(img.Spec.Ref(), img.Spec.Generation, img.Status.Observed)
	}
	return img, err
}

func (v *recordingView) KeyPairByName(name compute.KeyName) (compute.KeyPair, error) {
	kp, err := v.r.KeyPairByName(v.ctx, name)
	if err == nil {
		v.record(kp.Spec.Ref(), kp.Spec.Generation, kp.Observed())
	}
	return kp, err
}
