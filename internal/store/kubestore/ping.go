package kubestore

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// pingName is a ConfigMap name no resource uses. Ping reads it and expects
// NotFound, which needs only the get verb the store already holds.
const pingName = "vws-compute-health-probe"

// Ping reports whether the apiserver answers a read in the store's
// namespace. NotFound is success: the request was served and authorized.
func (s *Store) Ping(ctx context.Context) error {
	err := s.c.Get(ctx, client.ObjectKey{Namespace: s.ns, Name: pingName}, &corev1.ConfigMap{})
	if err == nil || apierrors.IsNotFound(err) {
		return nil
	}
	return fmt.Errorf("configmap store namespace %q: %w", s.ns, err)
}
