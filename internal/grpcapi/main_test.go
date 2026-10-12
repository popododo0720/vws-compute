package grpcapi_test

import (
	"os"
	"testing"

	"github.com/popododo0720/vws-compute/internal/store/kubestore/kubetest"
)

func TestMain(m *testing.M) { os.Exit(kubetest.Run(m)) }
