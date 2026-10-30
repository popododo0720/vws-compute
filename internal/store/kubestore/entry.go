package kubestore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/popododo0720/vws-lib/obs"

	"github.com/popododo0720/vws-compute/internal/compute"
)

const (
	labelKind = "vws.io/kind"
	labelOp   = "vws.io/op"
	recordKey = "record"

	kindRequest     = "request"
	kindClaimKey    = "claim-keyname"
	kindClaimVolume = "claim-volume"
)

// errCAS reports a lost optimistic-concurrency race on one ConfigMap. The
// caller rereads and reapplies its rule. It never leaves the package.
var errCAS = errors.New("kubestore: resourceVersion moved")

func isCAS(err error) bool {
	return errors.Is(err, errCAS)
}

func casErr(err error) error {
	if apierrors.IsConflict(err) || apierrors.IsAlreadyExists(err) {
		return errCAS
	}
	return err
}

func sum(s string) string {
	h := sha256.Sum256([]byte(s))
	return hex.EncodeToString(h[:])
}

func kindLabel(k compute.Kind) string { return k.String() }

func objectName(ref compute.Ref) string {
	return "vws-" + ref.Kind.String() + "-" + ref.ID
}

// entry is one resource's ConfigMap, decoded. Exactly one of inst, img, and
// key is meaningful, as ref.Kind says.
type entry struct {
	ref    compute.Ref
	inst   compute.Instance
	img    compute.Image
	key    compute.KeyPair
	ops    []compute.Operation
	rv     string
	exists bool
}

func (e *entry) generation() int64 {
	switch e.ref.Kind {
	case compute.KindInstance:
		return e.inst.Spec.Generation
	case compute.KindImage:
		return e.img.Spec.Generation
	default:
		return e.key.Spec.Generation
	}
}

func (e *entry) observed() compute.Observed {
	switch e.ref.Kind {
	case compute.KindInstance:
		return e.inst.Status.Observed
	case compute.KindImage:
		return e.img.Status.Observed
	default:
		return compute.KeyPair{Spec: e.key.Spec}.Observed()
	}
}

func (e *entry) revision() int64 {
	switch e.ref.Kind {
	case compute.KindInstance:
		return e.inst.Meta.Revision
	case compute.KindImage:
		return e.img.Meta.Revision
	default:
		return e.key.Meta.Revision
	}
}

func (e *entry) setMeta(m compute.Metadata) {
	switch e.ref.Kind {
	case compute.KindInstance:
		e.inst.Meta = m
	case compute.KindImage:
		e.img.Meta = m
	default:
		e.key.Meta = m
	}
}

// settleAll runs compute.Settle over every operation.
func (e *entry) settleAll(spec int64, obs compute.Observed, at time.Time) {
	for i, op := range e.ops {
		e.ops[i] = compute.Settle(op, spec, obs, at)
	}
}

func (e *entry) op(match func(compute.Operation) bool) (compute.Operation, bool) {
	for _, op := range e.ops {
		if match(op) {
			return op, true
		}
	}
	return compute.Operation{}, false
}

func decode(ref compute.Ref, cm *corev1.ConfigMap, legacy compute.ImageScope) (*entry, error) {
	e := &entry{ref: ref, rv: cm.ResourceVersion, exists: true}
	var rec recordJSON
	if err := json.Unmarshal([]byte(cm.Data[recordKey]), &rec); err != nil {
		return nil, corrupt(ref.Kind.String(), ref.ID, err)
	}
	var err error
	switch ref.Kind {
	case compute.KindInstance:
		if rec.Instance == nil {
			return nil, corrupt("instance", ref.ID, errors.New("no instance"))
		}
		e.inst, err = instanceFrom(rec.Instance)
	case compute.KindImage:
		if rec.Image == nil {
			return nil, corrupt("image", ref.ID, errors.New("no image"))
		}
		e.img, err = imageFrom(rec.Image, legacy)
	case compute.KindKeyPair:
		if rec.KeyPair == nil {
			return nil, corrupt("key pair", ref.ID, errors.New("no key pair"))
		}
		e.key, err = keyPairFrom(rec.KeyPair)
	}
	if err != nil {
		return nil, err
	}
	for _, j := range rec.Ops {
		op, err := opFrom(j)
		if err != nil {
			return nil, err
		}
		e.ops = append(e.ops, op)
	}
	return e, nil
}

func (e *entry) configMap(ns string) (*corev1.ConfigMap, error) {
	var rec recordJSON
	switch e.ref.Kind {
	case compute.KindInstance:
		rec.Instance = instanceTo(e.inst)
	case compute.KindImage:
		j, err := imageTo(e.img)
		if err != nil {
			return nil, err
		}
		rec.Image = j
	case compute.KindKeyPair:
		rec.KeyPair = keyPairTo(e.key)
	}
	for _, op := range e.ops {
		rec.Ops = append(rec.Ops, opTo(op))
	}
	data, err := encode(rec)
	if err != nil {
		return nil, err
	}
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name: objectName(e.ref), Namespace: ns, ResourceVersion: e.rv,
			Labels: map[string]string{labelKind: kindLabel(e.ref.Kind)},
		},
		Data: map[string]string{recordKey: data},
	}, nil
}

// get reads one resource. A missing ConfigMap is an entry with exists false.
func (s *Store) get(ctx context.Context, ref compute.Ref) (*entry, error) {
	var cm corev1.ConfigMap
	err := s.c.Get(ctx, client.ObjectKey{Namespace: s.ns, Name: objectName(ref)}, &cm)
	if apierrors.IsNotFound(err) {
		return &entry{ref: ref}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get %s: %w", objectName(ref), err)
	}
	return decode(ref, &cm, s.legacyImageScope)
}

// list reads every resource of one kind.
func (s *Store) list(ctx context.Context, kind compute.Kind) ([]*entry, error) {
	var cms corev1.ConfigMapList
	if err := s.c.List(ctx, &cms, client.InNamespace(s.ns), client.MatchingLabels{labelKind: kindLabel(kind)}); err != nil {
		return nil, fmt.Errorf("list %s: %w", kindLabel(kind), err)
	}
	out := make([]*entry, 0, len(cms.Items))
	prefix := "vws-" + kind.String() + "-"
	for i := range cms.Items {
		cm := &cms.Items[i]
		id := cm.Name[len(prefix):]
		e, err := decode(compute.Ref{Kind: kind, ID: id}, cm, s.legacyImageScope)
		if err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	slices.SortFunc(out, func(a, b *entry) int { return strings.Compare(a.ref.ID, b.ref.ID) })
	return out, nil
}

// write commits e with one Create or one Update. The resourceVersion read
// with e is the precondition, so a concurrent writer makes it fail with errCAS.
func (s *Store) write(ctx context.Context, e *entry) error {
	cm, err := e.configMap(s.ns)
	if err != nil {
		return err
	}
	if e.exists {
		return s.cas(ctx, s.c.Update(ctx, cm))
	}
	cm.ResourceVersion = ""
	return s.cas(ctx, s.c.Create(ctx, cm))
}

// cas turns a lost resourceVersion race into errCAS and counts it as
// vws.store.conflicts. The caller rereads and reapplies its rule.
func (s *Store) cas(ctx context.Context, err error) error {
	err = casErr(err)
	if isCAS(err) {
		obs.M().StoreConflict(ctx, "kubestore")
	}
	return err
}

const maxAttempts = 64

// update reads ref, applies fn, and writes the result with CAS, rereading
// and reapplying fn when another writer got there first. fn reports whether
// it changed e. It must depend only on e and its own inputs, because it runs
// once per attempt.
func (s *Store) update(ctx context.Context, ref compute.Ref, fn func(e *entry) (bool, error)) error {
	for range maxAttempts {
		if err := ctx.Err(); err != nil {
			return err
		}
		e, err := s.get(ctx, ref)
		if err != nil {
			return err
		}
		changed, err := fn(e)
		if err != nil || !changed {
			return err
		}
		err = s.write(ctx, e)
		if isCAS(err) {
			continue
		}
		return err
	}
	return compute.Errorf(compute.CodeAborted, "%s kept changing; retry with the same request id", ref)
}
