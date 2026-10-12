package kubestore

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/popododo0720/vws-compute/internal/compute"
)

// A claim is a ConfigMap that gives a subject (a key name or a volume) to one
// holder. Taking one is a create, or a CAS update when the current holder no
// longer qualifies. A missing holder qualifies: it may be a commit that has
// claimed and not yet written its resource, so only Repair, which runs
// before serving, removes such a claim.

type claim struct {
	kind    string
	subject string
	holder  string
}

func (c claim) name() string {
	return "vws-claim-" + c.kind[len("claim-"):] + "-" + sum(c.subject)[:40]
}

func (c claim) configMap(ns, rv string) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: c.name(), Namespace: ns, ResourceVersion: rv, Labels: map[string]string{labelKind: c.kind}},
		Data:       map[string]string{"subject": c.subject, "holder": c.holder},
	}
}

// acquire takes the claim for c.holder. It returns held=true when another
// holder qualifies, and fresh=true when it created or took over the claim.
func (s *Store) acquire(ctx context.Context, c claim, qualifies func(holder string) (bool, error)) (fresh, held bool, err error) {
	for range maxAttempts {
		var cur corev1.ConfigMap
		err := s.c.Get(ctx, client.ObjectKey{Namespace: s.ns, Name: c.name()}, &cur)
		switch {
		case apierrors.IsNotFound(err):
			err = casErr(s.c.Create(ctx, c.configMap(s.ns, "")))
		case err != nil:
			return false, false, err
		case cur.Data["holder"] == c.holder:
			return false, false, nil
		default:
			ok, qerr := qualifies(cur.Data["holder"])
			if qerr != nil {
				return false, false, qerr
			}
			if ok {
				return false, true, nil
			}
			err = casErr(s.c.Update(ctx, c.configMap(s.ns, cur.ResourceVersion)))
		}
		if err == nil {
			return true, false, nil
		}
		if !isCAS(err) {
			return false, false, err
		}
	}
	return false, false, compute.Errorf(compute.CodeAborted, "claim %s kept changing", c.subject)
}

// release deletes the claim if c.holder still holds it.
func (s *Store) release(ctx context.Context, c claim) error {
	var cur corev1.ConfigMap
	err := s.c.Get(ctx, client.ObjectKey{Namespace: s.ns, Name: c.name()}, &cur)
	if apierrors.IsNotFound(err) || (err == nil && cur.Data["holder"] != c.holder) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("get claim %s: %w", c.name(), err)
	}
	rv := cur.ResourceVersion
	err = s.c.Delete(ctx, &cur, client.Preconditions{ResourceVersion: &rv})
	if apierrors.IsNotFound(err) || apierrors.IsConflict(err) {
		return nil
	}
	return fmt.Errorf("delete claim %s: %w", c.name(), err)
}

// keyNameQualifies reports whether holder still holds the key name: it does
// unless that key pair exists and is deleted or has another name.
func (s *Store) keyNameQualifies(ctx context.Context, name string) func(string) (bool, error) {
	return func(holder string) (bool, error) {
		e, err := s.get(ctx, compute.Ref{Kind: compute.KindKeyPair, ID: holder})
		if err != nil || !e.exists {
			return true, err
		}
		return !e.key.Spec.Deleted && e.key.Spec.Name.String() == name, nil
	}
}

// volumeQualifies reports whether holder still holds the volume: it does
// unless that instance exists and is terminated or no longer lists it.
func (s *Store) volumeQualifies(ctx context.Context, volume string) func(string) (bool, error) {
	return func(holder string) (bool, error) {
		e, err := s.get(ctx, compute.Ref{Kind: compute.KindInstance, ID: holder})
		if err != nil || !e.exists {
			return true, err
		}
		if e.inst.Spec.Power == compute.PowerTerminated {
			return false, nil
		}
		for _, v := range e.inst.Spec.Volumes {
			if v.Volume.String() == volume {
				return true, nil
			}
		}
		return false, nil
	}
}
