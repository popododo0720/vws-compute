package kubestore

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/popododo0720/vws-compute/internal/compute"
)

// requestIndex is the ConfigMap that makes a request id unique. Creating it
// is the claim. It holds the fingerprint, so a replay is judged without
// reading the resource, and the target, so an operation id finds its
// resource.
type requestIndex struct {
	request     compute.RequestID
	fingerprint compute.Fingerprint
	op          compute.OperationID
	target      compute.Ref
	rv          string
}

func indexName(req compute.RequestID) string { return "vws-req-" + sum(req.String()) }

func (x *requestIndex) configMap(ns string) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name: indexName(x.request), Namespace: ns, ResourceVersion: x.rv,
			Labels: map[string]string{labelKind: kindRequest, labelOp: x.op.String()},
		},
		Data: map[string]string{
			"request":     x.request.String(),
			"fingerprint": hex.EncodeToString(x.fingerprint[:]),
			"op":          x.op.String(),
			"target-kind": strconv.Itoa(int(x.target.Kind)),
			"target-id":   x.target.ID,
		},
	}
}

func decodeIndex(cm *corev1.ConfigMap) (*requestIndex, error) {
	req, err1 := compute.ParseRequestID(cm.Data["request"])
	op, err2 := compute.ParseOperationID(cm.Data["op"])
	fp, err3 := hex.DecodeString(cm.Data["fingerprint"])
	kind, err4 := strconv.Atoi(cm.Data["target-kind"])
	for _, err := range []error{err1, err2, err3, err4} {
		if err != nil {
			return nil, corrupt("request index", cm.Name, err)
		}
	}
	x := &requestIndex{request: req, op: op, target: compute.Ref{Kind: compute.Kind(kind), ID: cm.Data["target-id"]}, rv: cm.ResourceVersion}
	if len(fp) != len(x.fingerprint) || kind < int(compute.KindInstance) || kind > int(compute.KindKeyPair) {
		return nil, corrupt("request index", cm.Name, errBadIndex)
	}
	copy(x.fingerprint[:], fp)
	return x, nil
}

func indexOf(op compute.Operation) *requestIndex {
	return &requestIndex{request: op.Request, fingerprint: op.Fingerprint, op: op.ID, target: op.Target}
}

// index reads the index of req, or nil when there is none.
func (s *Store) index(ctx context.Context, req compute.RequestID) (*requestIndex, error) {
	var cm corev1.ConfigMap
	err := s.c.Get(ctx, client.ObjectKey{Namespace: s.ns, Name: indexName(req)}, &cm)
	if apierrors.IsNotFound(err) {
		return nil, nil //nolint:nilnil // nil is "no index": callers branch on it and every path treats it as unclaimed
	}
	if err != nil {
		return nil, fmt.Errorf("get request index %s: %w", indexName(req), err)
	}
	return decodeIndex(&cm)
}

// indexByOp finds the index that names op, or nil.
func (s *Store) indexByOp(ctx context.Context, id compute.OperationID) (*requestIndex, error) {
	var cms corev1.ConfigMapList
	if err := s.c.List(ctx, &cms, client.InNamespace(s.ns), client.MatchingLabels{labelKind: kindRequest, labelOp: id.String()}); err != nil {
		return nil, fmt.Errorf("list request index for %s: %w", id, err)
	}
	for i := range cms.Items {
		return decodeIndex(&cms.Items[i])
	}
	return nil, nil //nolint:nilnil // nil is "no index names op", which callers handle
}

// claimIndex creates the index. It fails with errCAS when one exists.
func (s *Store) claimIndex(ctx context.Context, x *requestIndex) error {
	return casErr(s.c.Create(ctx, x.configMap(s.ns)))
}

// repoint makes the index of x.request name x.op. It is a CAS update.
func (s *Store) repoint(ctx context.Context, x *requestIndex) error {
	return casErr(s.c.Update(ctx, x.configMap(s.ns)))
}

// releaseIndex deletes the index, only if it still names op.
func (s *Store) releaseIndex(ctx context.Context, req compute.RequestID, op compute.OperationID) error {
	x, err := s.index(ctx, req)
	if err != nil || x == nil || x.op != op {
		return err
	}
	rv := x.rv
	err = s.c.Delete(ctx, x.configMap(s.ns), client.Preconditions{ResourceVersion: &rv})
	if apierrors.IsNotFound(err) || apierrors.IsConflict(err) {
		return nil
	}
	return fmt.Errorf("delete request index %s: %w", indexName(req), err)
}

var errBadIndex = errors.New("bad index fields")
