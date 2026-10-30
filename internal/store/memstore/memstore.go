// Package memstore is the in-memory Repository.
//
// State does not survive the process. A "restart" over memstore means
// building a new controller and reconciler over the same *Store, which is
// exactly the recovery path: neither holds anything the store does not.
package memstore

import (
	"context"
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/popododo0720/vws-compute/internal/compute"
	"github.com/popododo0720/vws-compute/internal/store"
)

var _ store.Repository = (*Store)(nil)

// Store is safe for concurrent use. One mutex serializes every commit, which
// gives the same isolation as SQLite's single writer. Rows hold deep copies,
// so no caller shares memory with a row.
type Store struct {
	mu sync.Mutex

	instances map[string]*instanceRow
	images    map[string]*imageRow
	keyPairs  map[string]keyPairRow

	ops         map[compute.OperationID]compute.Operation
	opByRequest map[compute.RequestID]compute.OperationID
	pending     map[compute.Ref]map[compute.OperationID]struct{}

	// legacyImageScope is stored for an image spec written without a scope
	// (a seed image). Zero leaves it in no project.
	legacyImageScope compute.ImageScope
}

type instanceRow struct {
	spec   compute.InstanceSpec
	status compute.InstanceStatus
	meta   compute.Metadata
}

type imageRow struct {
	spec   compute.ImageSpec
	status compute.ImageStatus
	meta   compute.Metadata
}

type keyPairRow struct {
	spec compute.KeyPairSpec
	meta compute.Metadata
}

// New returns an empty store.
func New() *Store {
	return &Store{
		instances:   map[string]*instanceRow{},
		images:      map[string]*imageRow{},
		keyPairs:    map[string]keyPairRow{},
		ops:         map[compute.OperationID]compute.Operation{},
		opByRequest: map[compute.RequestID]compute.OperationID{},
		pending:     map[compute.Ref]map[compute.OperationID]struct{}{},
	}
}

// WithLegacyImageScope sets the scope stored for image specs that carry none.
func (s *Store) WithLegacyImageScope(sc compute.ImageScope) *Store {
	s.legacyImageScope = sc
	return s
}

func (s *Store) Close() error { return nil }

func (s *Store) Instance(_ context.Context, id compute.InstanceID) (compute.Instance, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	row, ok := s.instances[id.String()]
	if !ok {
		return compute.Instance{}, compute.Errorf(compute.CodeNotFound, "instance %s not found", id)
	}
	return row.read(), nil
}

func (s *Store) Instances(_ context.Context, q store.InstanceQuery) (store.Page[compute.Instance], error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	keys := make([]string, 0, len(q.IDs))
	for _, id := range q.IDs {
		keys = append(keys, id.String())
	}
	ids := selectKeys(s.instances, keys, q.After)
	return page(ids, q.PageSize, func(id string) compute.Instance { return s.instances[id].read() }), nil
}

func (s *Store) Image(_ context.Context, id compute.ImageID) (compute.Image, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	row, ok := s.images[id.String()]
	if !ok {
		return compute.Image{}, compute.Errorf(compute.CodeNotFound, "image %s not found", id)
	}
	return row.read(), nil
}

func (s *Store) Images(_ context.Context, q store.ImageQuery) (store.Page[compute.Image], error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	keys := make([]string, 0, len(q.IDs))
	for _, id := range q.IDs {
		keys = append(keys, id.String())
	}
	ids := selectKeys(s.images, keys, q.After)
	return page(ids, q.PageSize, func(id string) compute.Image { return s.images[id].read() }), nil
}

func (s *Store) KeyPair(_ context.Context, id compute.KeyPairID) (compute.KeyPair, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	row, ok := s.keyPairs[id.String()]
	if !ok {
		return compute.KeyPair{}, compute.Errorf(compute.CodeNotFound, "key pair %s not found", id)
	}
	return row.read(), nil
}

func (s *Store) KeyPairByName(_ context.Context, name compute.KeyName) (compute.KeyPair, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var best keyPairRow
	found := false
	for _, row := range s.keyPairs {
		if row.spec.Name != name {
			continue
		}
		if !found || newerIncarnation(row.spec, best.spec) {
			best, found = row, true
		}
	}
	if !found {
		return compute.KeyPair{}, compute.Errorf(compute.CodeNotFound, "key pair %q not found", name)
	}
	return best.read(), nil
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

func (s *Store) KeyPairs(_ context.Context, q store.KeyPairQuery) (store.Page[compute.KeyPair], error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	live := map[string]keyPairRow{}
	for _, row := range s.keyPairs {
		if !row.spec.Deleted {
			live[row.spec.Name.String()] = row
		}
	}
	keys := make([]string, 0, len(q.Names))
	for _, n := range q.Names {
		keys = append(keys, n.String())
	}
	names := selectKeys(live, keys, q.After)
	return page(names, q.PageSize, func(n string) compute.KeyPair { return live[n].read() }), nil
}

func (s *Store) Operation(_ context.Context, id compute.OperationID) (compute.Operation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	op, ok := s.ops[id]
	if !ok {
		return compute.Operation{}, compute.Errorf(compute.CodeNotFound, "operation %s not found", id)
	}
	return op, nil
}

func (s *Store) OperationByRequest(_ context.Context, req compute.RequestID) (compute.Operation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	id, ok := s.opByRequest[req]
	if !ok {
		return compute.Operation{}, compute.Errorf(compute.CodeNotFound, "no operation for request %s", req)
	}
	return s.ops[id], nil
}

func (s *Store) CommitDesired(_ context.Context, c store.DesiredChange) (compute.Operation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if id, ok := s.opByRequest[c.Op.Request]; ok {
		existing := s.ops[id]
		if existing.Fingerprint != c.Op.Fingerprint {
			return compute.Operation{}, compute.Errorf(compute.CodeRequestMismatch, "request id %s was used for a different request", c.Op.Request)
		}
		return existing, nil
	}
	for _, v := range c.Expect {
		gen, obs, _ := s.progress(v.Ref)
		if !v.Matches(gen, obs) {
			return compute.Operation{}, store.ErrConflict
		}
	}
	ref := c.Spec.Ref()
	if ref != c.Op.Target || c.Spec.Gen() != c.Op.TargetGeneration {
		return compute.Operation{}, compute.Errorf(compute.CodeInternal, "operation %s does not target the change", c.Op.ID)
	}
	cur, _, _ := s.progress(ref)
	if c.Write {
		if c.Spec.Gen() != cur+1 {
			return compute.Operation{}, store.ErrConflict
		}
	} else if c.Spec.Gen() != cur {
		return compute.Operation{}, store.ErrConflict
	}
	if c.Meta != nil {
		if err := store.CheckMetadata(*c.Meta, cur == 0, s.revision(ref)); err != nil {
			return compute.Operation{}, err
		}
	}
	if c.Write {
		if err := s.put(c.Spec); err != nil {
			return compute.Operation{}, err
		}
	}
	if c.Meta != nil {
		if err := s.putMeta(ref, *c.Meta); err != nil {
			return compute.Operation{}, err
		}
	}
	spec, obs, _ := s.progress(ref)
	if c.Write {
		s.settlePending(ref, spec, obs, c.Op.CreatedAt)
	}
	op := compute.Settle(c.Op, spec, obs, c.Op.CreatedAt)
	s.ops[op.ID] = op
	s.opByRequest[op.Request] = op.ID
	if op.State() == compute.OperationPending {
		if s.pending[ref] == nil {
			s.pending[ref] = map[compute.OperationID]struct{}{}
		}
		s.pending[ref][op.ID] = struct{}{}
	}
	return op, nil
}

func (s *Store) put(spec compute.Spec) error {
	switch spec := spec.(type) {
	case compute.InstanceSpec:
		row, ok := s.instances[spec.ID.String()]
		if spec.ControllerDriven() && spec.Power != compute.PowerTerminated {
			if err := s.checkVolumes(spec); err != nil {
				return err
			}
		}
		if !ok {
			row = &instanceRow{}
			s.instances[spec.ID.String()] = row
		} else if spec.Generation > row.spec.Generation {
			row.status.Retry = compute.Retry{}
		}
		row.spec = cloneInstanceSpec(spec)
	case compute.ImageSpec:
		row, ok := s.images[spec.ID.String()]
		if !ok {
			row = &imageRow{}
			s.images[spec.ID.String()] = row
		} else if spec.Generation > row.spec.Generation {
			row.status.Retry = compute.Retry{}
		}
		if spec.Scope.IsZero() {
			spec.Scope = s.legacyImageScope
		}
		row.spec = spec
	case compute.KeyPairSpec:
		if !spec.Deleted {
			for id, other := range s.keyPairs {
				if id != spec.ID.String() && other.spec.Name == spec.Name && !other.spec.Deleted {
					return store.ErrConflict
				}
			}
		}
		row := s.keyPairs[spec.ID.String()]
		row.spec = spec
		s.keyPairs[spec.ID.String()] = row
	default:
		return compute.Errorf(compute.CodeInternal, "unknown spec %T", spec)
	}
	return nil
}

// progress returns a resource's spec generation (0 if absent) and observed
// state.
func (s *Store) progress(ref compute.Ref) (int64, compute.Observed, bool) {
	switch ref.Kind {
	case compute.KindInstance:
		if row, ok := s.instances[ref.ID]; ok {
			return row.spec.Generation, row.status.Observed, true
		}
	case compute.KindImage:
		if row, ok := s.images[ref.ID]; ok {
			return row.spec.Generation, row.status.Observed, true
		}
	case compute.KindKeyPair:
		if row, ok := s.keyPairs[ref.ID]; ok {
			return row.spec.Generation, compute.KeyPair{Spec: row.spec}.Observed(), true
		}
	}
	return 0, compute.Observed{}, false
}

func (s *Store) settlePending(ref compute.Ref, spec int64, obs compute.Observed, now time.Time) {
	for id := range s.pending[ref] {
		op := compute.Settle(s.ops[id], spec, obs, now)
		s.ops[id] = op
		if op.State().Terminal() {
			delete(s.pending[ref], id)
		}
	}
	if len(s.pending[ref]) == 0 {
		delete(s.pending, ref)
	}
}

func (s *Store) CommitInstanceStatus(_ context.Context, id compute.InstanceID, st compute.InstanceStatus) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	row, ok := s.instances[id.String()]
	if !ok {
		return compute.Errorf(compute.CodeNotFound, "instance %s not found", id)
	}
	if err := store.CheckObservation(st.Observed, row.status.Observed, row.spec.Generation, st.UpdatedAt); err != nil {
		return err
	}
	st.Report = row.status.Report
	row.status = cloneInstanceStatus(st)
	s.settlePending(id.Ref(), row.spec.Generation, row.status.Observed, st.UpdatedAt)
	return nil
}

func (s *Store) CommitImageStatus(_ context.Context, id compute.ImageID, st compute.ImageStatus) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	row, ok := s.images[id.String()]
	if !ok {
		return compute.Errorf(compute.CodeNotFound, "image %s not found", id)
	}
	if err := store.CheckObservation(st.Observed, row.status.Observed, row.spec.Generation, st.UpdatedAt); err != nil {
		return err
	}
	row.status = cloneImageStatus(st)
	s.settlePending(id.Ref(), row.spec.Generation, row.status.Observed, st.UpdatedAt)
	return nil
}

func (s *Store) Unconverged(_ context.Context, now time.Time) ([]compute.Ref, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.laggingLocked(func(retry compute.Retry) bool { return retry.Due(now) }), nil
}

func (s *Store) Lagging(context.Context) ([]compute.Ref, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.laggingLocked(func(compute.Retry) bool { return true }), nil
}

func (s *Store) laggingLocked(include func(compute.Retry) bool) []compute.Ref {
	var inst, img []string
	for id, row := range s.instances {
		if !row.spec.ControllerDriven() && row.spec.Generation > row.status.Observed.Generation && include(row.status.Retry) {
			inst = append(inst, id)
		}
	}
	for id, row := range s.images {
		if row.spec.Generation > row.status.Observed.Generation && include(row.status.Retry) {
			img = append(img, id)
		}
	}
	sort.Strings(inst)
	sort.Strings(img)
	refs := make([]compute.Ref, 0, len(inst)+len(img))
	for _, id := range inst {
		refs = append(refs, compute.Ref{Kind: compute.KindInstance, ID: id})
	}
	for _, id := range img {
		refs = append(refs, compute.Ref{Kind: compute.KindImage, ID: id})
	}
	return refs
}

func (r *instanceRow) read() compute.Instance {
	return compute.Instance{Spec: cloneInstanceSpec(r.spec), Status: cloneInstanceStatus(r.status), Meta: r.meta}
}

func (r *imageRow) read() compute.Image {
	return compute.Image{Spec: r.spec, Status: cloneImageStatus(r.status), Meta: r.meta}
}

func (r keyPairRow) read() compute.KeyPair {
	return compute.KeyPair{Spec: r.spec, Meta: r.meta}
}

func (s *Store) revision(ref compute.Ref) int64 {
	switch ref.Kind {
	case compute.KindInstance:
		if row, ok := s.instances[ref.ID]; ok {
			return row.meta.Revision
		}
	case compute.KindImage:
		if row, ok := s.images[ref.ID]; ok {
			return row.meta.Revision
		}
	case compute.KindKeyPair:
		if row, ok := s.keyPairs[ref.ID]; ok {
			return row.meta.Revision
		}
	}
	return 0
}

func (s *Store) putMeta(ref compute.Ref, m compute.Metadata) error {
	switch ref.Kind {
	case compute.KindInstance:
		row, ok := s.instances[ref.ID]
		if !ok {
			return store.ErrConflict
		}
		row.meta = m
	case compute.KindImage:
		row, ok := s.images[ref.ID]
		if !ok {
			return store.ErrConflict
		}
		row.meta = m
	case compute.KindKeyPair:
		row, ok := s.keyPairs[ref.ID]
		if !ok {
			return store.ErrConflict
		}
		row.meta = m
		s.keyPairs[ref.ID] = row
	default:
		return compute.Errorf(compute.CodeInternal, "unknown resource kind %d", ref.Kind)
	}
	return nil
}

func cloneInstanceSpec(s compute.InstanceSpec) compute.InstanceSpec {
	if s.Key != nil {
		k := *s.Key
		s.Key = &k
	}
	s.Volumes = slices.Clone(s.Volumes)
	if s.NICs != nil {
		nics := make([]compute.NetworkInterface, len(s.NICs))
		for i, n := range s.NICs {
			n.SecurityGroups = slices.Clone(n.SecurityGroups)
			nics[i] = n
		}
		s.NICs = nics
	}
	return s
}

func cloneInstanceStatus(st compute.InstanceStatus) compute.InstanceStatus {
	st.Observed = cloneObserved(st.Observed)
	st.Report.Interfaces = slices.Clone(st.Report.Interfaces)
	st.Report.Volumes = slices.Clone(st.Report.Volumes)
	return st
}

func cloneImageStatus(st compute.ImageStatus) compute.ImageStatus {
	st.Observed = cloneObserved(st.Observed)
	return st
}

func cloneObserved(o compute.Observed) compute.Observed {
	if o.Failure != nil {
		f := *o.Failure
		o.Failure = &f
	}
	if o.AttachmentFailure != nil {
		f := *o.AttachmentFailure
		o.AttachmentFailure = &f
	}
	return o
}

// selectKeys returns the sorted keys of m greater than after, restricted to
// only when only is non-empty.
func selectKeys[V any](m map[string]V, only []string, after string) []string {
	var keys []string
	if len(only) > 0 {
		seen := map[string]bool{}
		for _, k := range only {
			if _, ok := m[k]; ok && !seen[k] && k > after {
				keys, seen[k] = append(keys, k), true
			}
		}
	} else {
		for k := range m {
			if k > after {
				keys = append(keys, k)
			}
		}
	}
	sort.Strings(keys)
	return keys
}

func page[T any](keys []string, size int, read func(string) T) store.Page[T] {
	limit := store.PageLimit(size)
	var p store.Page[T]
	if len(keys) > limit {
		keys = keys[:limit]
		p.Next = keys[limit-1]
	}
	p.Items = make([]T, 0, len(keys))
	for _, k := range keys {
		p.Items = append(p.Items, read(k))
	}
	return p
}
