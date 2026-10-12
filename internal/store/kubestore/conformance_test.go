package kubestore_test

import (
	"testing"

	"github.com/popododo0720/vws-compute/internal/store"
	"github.com/popododo0720/vws-compute/internal/store/kubestore/kubetest"
	"github.com/popododo0720/vws-compute/internal/store/storetest"
)

func TestConformance(t *testing.T) {
	storetest.Run(t, func(t *testing.T) store.Repository { return kubetest.Store(t) })
}
