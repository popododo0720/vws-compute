package kubestore

import (
	"context"
	"slices"
	"strings"

	"github.com/popododo0720/vws-compute/internal/compute"
	"github.com/popododo0720/vws-compute/internal/store"
)

// CommitDesired follows the doc comment on store.DesiredStore. The order is:
// read the index, check read-only, claim the index, take claims, then one
// CAS update of the resource. Every check runs again inside the CAS loop on
// fresh state, so a writer that slipped in between turns into ErrConflict
// and nothing is written.
func (s *Store) CommitDesired(ctx context.Context, c store.DesiredChange) (compute.Operation, error) {
	defer s.lock(c.Op.Request)()
	ref := c.Spec.Ref()
	if ref != c.Op.Target || c.Spec.Gen() != c.Op.TargetGeneration {
		return compute.Operation{}, compute.Errorf(compute.CodeInternal, "operation %s does not target the change", c.Op.ID)
	}
	var idx *requestIndex
	claimed := false
	for range 3 {
		var err error
		if idx, err = s.index(ctx, c.Op.Request); err != nil {
			return compute.Operation{}, err
		}
		if idx != nil {
			if idx.fingerprint != c.Op.Fingerprint {
				return compute.Operation{}, compute.Errorf(compute.CodeRequestMismatch, "request id %s was used for a different request", c.Op.Request)
			}
			if op, ok, err := s.opOf(ctx, idx); err != nil || ok {
				return op, err
			}
			// The index names an operation the resource never took: a crash
			// between the two writes. Take it over.
		}
		// Check before claiming, so a plain conflict writes nothing.
		if _, _, err := s.apply(ctx, c, ref, nil); err != nil {
			return compute.Operation{}, err
		}
		if idx != nil {
			next := *idx
			next.op, next.target = c.Op.ID, c.Op.Target
			if err := s.repoint(ctx, &next); err != nil {
				if isCAS(err) {
					continue
				}
				return compute.Operation{}, err
			}
			claimed = true
			break
		}
		if err := s.claimIndex(ctx, indexOf(c.Op)); err != nil {
			if isCAS(err) {
				continue
			}
			return compute.Operation{}, err
		}
		claimed = true
		break
	}
	if !claimed {
		return compute.Operation{}, compute.Errorf(compute.CodeAborted, "request id %s kept changing; retry with the same request id", c.Op.Request)
	}
	op, err := s.finish(ctx, c, ref)
	if err != nil {
		// Give the claim back. A failed release is harmless: Repair, or the
		// next attempt with this request id, takes over a stale index.
		_ = s.releaseIndex(ctx, c.Op.Request, c.Op.ID) //nolint:errcheck // best effort: Repair, or the next attempt with this request id, takes over what is left
		return compute.Operation{}, err
	}
	if op.ID != c.Op.ID {
		// A racer's operation won. Point the index at what the resource holds.
		if x, err := s.index(ctx, c.Op.Request); err == nil && x != nil && x.op != op.ID {
			next := *x
			next.op = op.ID
			_ = s.repoint(ctx, &next) //nolint:errcheck // best effort: Repair, or the next attempt with this request id, takes over what is left
		}
	}
	return op, nil
}

// finish takes the claims the change needs and commits the resource.
func (s *Store) finish(ctx context.Context, c store.DesiredChange, ref compute.Ref) (compute.Operation, error) {
	var taken []claim
	giveBack := func() {
		for _, cl := range taken {
			_ = s.release(ctx, cl) //nolint:errcheck // best effort: a stale claim is taken over once its holder no longer qualifies
		}
	}
	claims, qualifies, err := s.claimsFor(ctx, c)
	if err != nil {
		return compute.Operation{}, err
	}
	for _, cl := range claims {
		fresh, held, err := s.acquire(ctx, cl, qualifies(cl))
		if err != nil {
			giveBack()
			return compute.Operation{}, err
		}
		if held {
			giveBack()
			if cl.kind == kindClaimKey {
				return compute.Operation{}, store.ErrConflict
			}
			return compute.Operation{}, compute.Errorf(compute.CodeFailedPrecondition, "volume %s is already attached to another instance", cl.subject)
		}
		if fresh {
			taken = append(taken, cl)
		}
	}
	op, written, err := s.apply(ctx, c, ref, &c.Op)
	if err != nil || !written {
		// Nothing was written, either because a rule failed or because the
		// resource already holds this request's operation.
		giveBack()
	}
	return op, err
}

// claimsFor lists the claims c needs before its resource may be written.
// A live key pair claims its name. A new controller-driven instance claims
// each volume, in sorted order so two creators cannot deadlock.
func (s *Store) claimsFor(ctx context.Context, c store.DesiredChange) ([]claim, func(claim) func(string) (bool, error), error) {
	qualifies := func(cl claim) func(string) (bool, error) {
		if cl.kind == kindClaimKey {
			return s.keyNameQualifies(ctx, cl.subject)
		}
		return s.volumeQualifies(ctx, cl.subject)
	}
	if !c.Write {
		return nil, qualifies, nil
	}
	switch spec := c.Spec.(type) {
	case compute.KeyPairSpec:
		if !spec.Deleted {
			return []claim{{kind: kindClaimKey, subject: spec.Name.String(), holder: spec.ID.String()}}, qualifies, nil
		}
	case compute.InstanceSpec:
		e, err := s.get(ctx, spec.Ref())
		if err != nil {
			return nil, nil, err
		}
		if !spec.ControllerDriven() || spec.Power == compute.PowerTerminated {
			return nil, qualifies, nil
		}
		// A create claims every volume; an update claims only the ones it
		// attaches. A detached volume's claim is taken over later, because
		// volumeQualifies no longer finds it on this instance.
		var out []claim
		for _, v := range spec.Volumes {
			if e.exists && holds(e.inst.Spec.Volumes, v.Volume) {
				continue
			}
			out = append(out, claim{kind: kindClaimVolume, subject: v.Volume.String(), holder: spec.ID.String()})
		}
		slices.SortFunc(out, func(a, b claim) int { return strings.Compare(a.subject, b.subject) })
		return slices.CompactFunc(out, func(a, b claim) bool { return a.subject == b.subject }), qualifies, nil
	}
	return nil, qualifies, nil
}

// apply runs the commit rule against the resource. With write nil it only
// checks. Otherwise it commits op with CAS and reports whether it wrote. When
// the resource already holds an operation for the request, that operation is
// returned and nothing is written.
func (s *Store) apply(ctx context.Context, c store.DesiredChange, ref compute.Ref, write *compute.Operation) (compute.Operation, bool, error) {
	var out compute.Operation
	var pruned []compute.Operation
	wrote := false
	err := s.update(ctx, ref, func(e *entry) (bool, error) {
		wrote, pruned = false, nil
		if prior, ok := e.op(func(o compute.Operation) bool { return o.Request == c.Op.Request }); ok {
			if prior.Fingerprint != c.Op.Fingerprint {
				return false, compute.Errorf(compute.CodeRequestMismatch, "request id %s was used for a different request", c.Op.Request)
			}
			out = prior
			return false, nil
		}
		for _, v := range c.Expect {
			var gen int64
			var obs compute.Observed
			if v.Ref == ref {
				gen, obs = e.generation(), e.observed()
			} else {
				other, err := s.get(ctx, v.Ref)
				if err != nil {
					return false, err
				}
				gen, obs = other.generation(), other.observed()
			}
			if !v.Matches(gen, obs) {
				return false, store.ErrConflict
			}
		}
		cur := e.generation()
		if c.Write {
			if c.Spec.Gen() != cur+1 {
				return false, store.ErrConflict
			}
		} else if c.Spec.Gen() != cur {
			return false, store.ErrConflict
		}
		if c.Meta != nil {
			if err := store.CheckMetadata(*c.Meta, cur == 0, e.revision()); err != nil {
				return false, err
			}
		}
		if write == nil {
			return false, nil
		}
		if c.Write {
			if err := put(e, c.Spec); err != nil {
				return false, err
			}
		}
		if c.Meta != nil {
			e.setMeta(*c.Meta)
		}
		if c.Write {
			e.settleAll(e.generation(), e.observed(), c.Op.CreatedAt)
		}
		op := compute.Settle(c.Op, e.generation(), e.observed(), c.Op.CreatedAt)
		e.ops = append(e.ops, op)
		pruned = e.prune(c.Op.CreatedAt)
		out, wrote = op, true
		return true, nil
	})
	if err == nil && wrote {
		// The resource no longer holds these operations, so their request
		// ids are forgotten. A failed delete is harmless: Repair removes an
		// index whose operation the resource does not hold.
		for _, op := range pruned {
			_ = s.releaseIndex(ctx, op.Request, op.ID) //nolint:errcheck // best effort: Repair removes what is left
		}
	}
	return out, wrote, err
}

// put installs spec, leaving status and metadata as they are. A new
// generation clears the retry state, since backoff belongs to the previous
// attempt.
func put(e *entry, spec compute.Spec) error {
	switch spec := spec.(type) {
	case compute.InstanceSpec:
		if e.exists && spec.Generation > e.inst.Spec.Generation {
			e.inst.Status.Retry = compute.Retry{}
		}
		e.inst.Spec = spec
	case compute.ImageSpec:
		if e.exists && spec.Generation > e.img.Spec.Generation {
			e.img.Status.Retry = compute.Retry{}
		}
		e.img.Spec = spec
	case compute.KeyPairSpec:
		e.key.Spec = spec
	default:
		return compute.Errorf(compute.CodeInternal, "unknown spec %T", spec)
	}
	return nil
}

func holds(vols []compute.VolumeAttachment, ref compute.VolumeRef) bool {
	for _, v := range vols {
		if v.Volume == ref {
			return true
		}
	}
	return false
}
