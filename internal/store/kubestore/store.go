// Package kubestore is the Repository on plain Kubernetes ConfigMaps.
//
// There is no CRD. Each resource (instance, image, key pair) is one
// ConfigMap holding its spec, status, metadata, and operations as JSON, and
// every write to it is a compare-and-swap on its resourceVersion. A commit is
// therefore atomic within one object. A request id is one more ConfigMap,
// created if it does not exist, so object-name uniqueness makes the claim
// atomic.
//
// Two writes cannot be atomic: the request-id index and the resource. A
// commit claims the index first and then updates the resource. A crash
// between the two leaves an index whose operation the resource does not
// hold. Repair removes it, and a retry with the same request id adopts it.
// Nothing else can be half done, because the resource holds all other state.
//
// Key name uniqueness and volume ownership are claim ConfigMaps. A claim is
// taken before the resource write and is only ever taken over from a holder
// that exists and no longer qualifies.
package kubestore

import (
	"context"
	"hash/fnv"
	"sync"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/popododo0720/vws-compute/internal/compute"
	"github.com/popododo0720/vws-compute/internal/store"
)

var _ store.Repository = (*Store)(nil)

// Store is safe for concurrent use. It holds no state besides the client.
type Store struct {
	c  client.Client
	ns string
	// locks serialize commits that share a request id inside this process.
	// They do not protect the data, CAS does. They keep two racers from
	// adopting the same orphaned index.
	locks [64]sync.Mutex
}

// New returns a store over namespace ns. It does not touch the cluster.
// Call Repair once before serving.
func New(c client.Client, ns string) *Store { return &Store{c: c, ns: ns} }

func (s *Store) Close() error { return nil }

func (s *Store) lock(req compute.RequestID) func() {
	h := fnv.New32a()
	_, _ = h.Write([]byte(req.String()))
	m := &s.locks[h.Sum32()%uint32(len(s.locks))]
	m.Lock()
	return m.Unlock
}

func (s *Store) Instance(ctx context.Context, id compute.InstanceID) (compute.Instance, error) {
	e, err := s.get(ctx, id.Ref())
	if err != nil {
		return compute.Instance{}, err
	}
	if !e.exists {
		return compute.Instance{}, compute.Errorf(compute.CodeNotFound, "instance %s not found", id)
	}
	return e.inst, nil
}

func (s *Store) Instances(ctx context.Context, q store.InstanceQuery) (store.Page[compute.Instance], error) {
	es, err := s.list(ctx, compute.KindInstance)
	if err != nil {
		return store.Page[compute.Instance]{}, err
	}
	only := make([]string, 0, len(q.IDs))
	for _, id := range q.IDs {
		only = append(only, id.String())
	}
	return pageOf(es, only, q.After, q.PageSize, func(e *entry) (string, compute.Instance) { return e.ref.ID, e.inst }), nil
}

func (s *Store) Image(ctx context.Context, id compute.ImageID) (compute.Image, error) {
	e, err := s.get(ctx, id.Ref())
	if err != nil {
		return compute.Image{}, err
	}
	if !e.exists {
		return compute.Image{}, compute.Errorf(compute.CodeNotFound, "image %s not found", id)
	}
	return e.img, nil
}

func (s *Store) Images(ctx context.Context, q store.ImageQuery) (store.Page[compute.Image], error) {
	es, err := s.list(ctx, compute.KindImage)
	if err != nil {
		return store.Page[compute.Image]{}, err
	}
	only := make([]string, 0, len(q.IDs))
	for _, id := range q.IDs {
		only = append(only, id.String())
	}
	return pageOf(es, only, q.After, q.PageSize, func(e *entry) (string, compute.Image) { return e.ref.ID, e.img }), nil
}

func (s *Store) KeyPair(ctx context.Context, id compute.KeyPairID) (compute.KeyPair, error) {
	e, err := s.get(ctx, id.Ref())
	if err != nil {
		return compute.KeyPair{}, err
	}
	if !e.exists {
		return compute.KeyPair{}, compute.Errorf(compute.CodeNotFound, "key pair %s not found", id)
	}
	return e.key, nil
}

func (s *Store) KeyPairByName(ctx context.Context, name compute.KeyName) (compute.KeyPair, error) {
	es, err := s.list(ctx, compute.KindKeyPair)
	if err != nil {
		return compute.KeyPair{}, err
	}
	var best compute.KeyPair
	found := false
	for _, e := range es {
		if e.key.Spec.Name != name {
			continue
		}
		if !found || newerIncarnation(e.key.Spec, best.Spec) {
			best, found = e.key, true
		}
	}
	if !found {
		return compute.KeyPair{}, compute.Errorf(compute.CodeNotFound, "key pair %q not found", name)
	}
	return best, nil
}

// newerIncarnation orders key pairs sharing a name: live first, then the
// latest ChangedAt, then the greater ID.
func newerIncarnation(a, b compute.KeyPairSpec) bool {
	if a.Deleted != b.Deleted {
		return !a.Deleted
	}
	if !a.ChangedAt.Equal(b.ChangedAt) {
		return a.ChangedAt.After(b.ChangedAt)
	}
	return a.ID.String() > b.ID.String()
}

func (s *Store) KeyPairs(ctx context.Context, q store.KeyPairQuery) (store.Page[compute.KeyPair], error) {
	es, err := s.list(ctx, compute.KindKeyPair)
	if err != nil {
		return store.Page[compute.KeyPair]{}, err
	}
	live := make([]*entry, 0, len(es))
	for _, e := range es {
		if !e.key.Spec.Deleted {
			live = append(live, e)
		}
	}
	only := make([]string, 0, len(q.Names))
	for _, n := range q.Names {
		only = append(only, n.String())
	}
	// Key pairs page by name, and at most one live key pair holds a name.
	return pageOf(live, only, q.After, q.PageSize, func(e *entry) (string, compute.KeyPair) { return e.key.Spec.Name.String(), e.key }), nil
}

func (s *Store) Operation(ctx context.Context, id compute.OperationID) (compute.Operation, error) {
	idx, err := s.indexByOp(ctx, id)
	if err != nil {
		return compute.Operation{}, err
	}
	if idx == nil {
		return compute.Operation{}, compute.Errorf(compute.CodeNotFound, "operation %s not found", id)
	}
	e, err := s.get(ctx, idx.target)
	if err != nil {
		return compute.Operation{}, err
	}
	if op, ok := e.op(func(o compute.Operation) bool { return o.ID == id }); ok {
		return op, nil
	}
	return compute.Operation{}, compute.Errorf(compute.CodeNotFound, "operation %s not found", id)
}

func (s *Store) OperationByRequest(ctx context.Context, req compute.RequestID) (compute.Operation, error) {
	idx, err := s.index(ctx, req)
	if err != nil {
		return compute.Operation{}, err
	}
	if idx != nil {
		if op, ok, err := s.opOf(ctx, idx); err != nil || ok {
			return op, err
		}
	}
	return compute.Operation{}, compute.Errorf(compute.CodeNotFound, "no operation for request %s", req)
}

// opOf returns the operation the index points at, if the resource holds it.
func (s *Store) opOf(ctx context.Context, idx *requestIndex) (compute.Operation, bool, error) {
	e, err := s.get(ctx, idx.target)
	if err != nil {
		return compute.Operation{}, false, err
	}
	op, ok := e.op(func(o compute.Operation) bool { return o.Request == idx.request })
	return op, ok, nil
}

func (s *Store) CommitInstanceStatus(ctx context.Context, id compute.InstanceID, st compute.InstanceStatus) error {
	return s.update(ctx, id.Ref(), func(e *entry) (bool, error) {
		if !e.exists {
			return false, compute.Errorf(compute.CodeNotFound, "instance %s not found", id)
		}
		if err := store.CheckObservation(st.Observed, e.inst.Status.Observed, e.inst.Spec.Generation, st.UpdatedAt); err != nil {
			return false, err
		}
		st.Report = e.inst.Status.Report
		e.inst.Status = st
		e.settleAll(e.inst.Spec.Generation, st.Observed, st.UpdatedAt)
		return true, nil
	})
}

func (s *Store) CommitImageStatus(ctx context.Context, id compute.ImageID, st compute.ImageStatus) error {
	return s.update(ctx, id.Ref(), func(e *entry) (bool, error) {
		if !e.exists {
			return false, compute.Errorf(compute.CodeNotFound, "image %s not found", id)
		}
		if err := store.CheckObservation(st.Observed, e.img.Status.Observed, e.img.Spec.Generation, st.UpdatedAt); err != nil {
			return false, err
		}
		e.img.Status = st
		e.settleAll(e.img.Spec.Generation, st.Observed, st.UpdatedAt)
		return true, nil
	})
}

func (s *Store) ReportInstanceStatus(ctx context.Context, id compute.InstanceID, rep compute.InstanceReport, now time.Time) (compute.Instance, error) {
	var out compute.Instance
	err := s.update(ctx, id.Ref(), func(e *entry) (bool, error) {
		if !e.exists {
			return false, compute.Errorf(compute.CodeNotFound, "instance %s not found", id)
		}
		next, changed, err := compute.ApplyReport(e.inst, rep, now)
		if err != nil {
			return false, err
		}
		if changed {
			if err := store.CheckObservation(next.Observed, e.inst.Status.Observed, e.inst.Spec.Generation, now); err != nil {
				return false, err
			}
			e.inst.Status = next
			e.settleAll(e.inst.Spec.Generation, e.inst.Status.Observed, now)
		}
		out = e.inst
		return changed, nil
	})
	return out, err
}

func (s *Store) Unconverged(ctx context.Context, now time.Time) ([]compute.Ref, error) {
	return s.lagging(ctx, func(r compute.Retry) bool { return r.Due(now) })
}

func (s *Store) Lagging(ctx context.Context) ([]compute.Ref, error) {
	return s.lagging(ctx, func(compute.Retry) bool { return true })
}

func (s *Store) lagging(ctx context.Context, include func(compute.Retry) bool) ([]compute.Ref, error) {
	insts, err := s.list(ctx, compute.KindInstance)
	if err != nil {
		return nil, err
	}
	imgs, err := s.list(ctx, compute.KindImage)
	if err != nil {
		return nil, err
	}
	refs := []compute.Ref{}
	for _, e := range insts {
		if !e.inst.Spec.ControllerDriven() && e.inst.Spec.Generation > e.inst.Status.Observed.Generation && include(e.inst.Status.Retry) {
			refs = append(refs, e.ref)
		}
	}
	for _, e := range imgs {
		if e.img.Spec.Generation > e.img.Status.Observed.Generation && include(e.img.Status.Retry) {
			refs = append(refs, e.ref)
		}
	}
	return refs, nil
}
