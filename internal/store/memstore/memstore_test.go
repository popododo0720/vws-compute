package memstore_test

import (
	"testing"

	"github.com/popododo0720/vws-compute/internal/store"
	"github.com/popododo0720/vws-compute/internal/store/memstore"
	"github.com/popododo0720/vws-compute/internal/store/storetest"
)

func TestConformance(t *testing.T) {
	storetest.Run(t, func(*testing.T) store.Repository { return memstore.New() })
}
