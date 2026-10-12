package kubevirt

import (
	"context"
	"errors"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// pingName is a PVC name no volume or image uses. Ping reads it and expects
// NotFound, which needs only the persistentvolumeclaims get verb the agent
// already holds in both namespaces (deploy/rbac/kubevirt-role.yaml).
const pingName = "vws-compute-health-probe"

// Ping reports whether the apiserver answers a read in the VM namespace and
// in the image namespace. NotFound is success.
func (a *Agent) Ping(ctx context.Context) error {
	var errs []error
	for _, ns := range []string{a.cfg.Namespace, a.cfg.ImageNamespace} {
		err := a.c.Get(ctx, client.ObjectKey{Namespace: ns, Name: pingName}, &corev1.PersistentVolumeClaim{})
		if err != nil && !apierrors.IsNotFound(err) {
			errs = append(errs, fmt.Errorf("namespace %q: %w", ns, err))
		}
	}
	return errors.Join(errs...)
}
