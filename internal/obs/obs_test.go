package obs_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/popododo0720/vws-compute/internal/obs"
)

func get(t *testing.T, srv *httptest.Server, path string) (int, string) {
	t.Helper()
	resp, err := srv.Client().Get(srv.URL + path) //nolint:noctx // test against a local server
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, string(b)
}

func TestProbesAndMetrics(t *testing.T) {
	var ready atomic.Bool
	m := obs.NewMetrics(ready.Load)
	srv := httptest.NewServer(obs.Handler(ready.Load, m))
	defer srv.Close()

	if code, _ := get(t, srv, "/healthz"); code != http.StatusOK {
		t.Fatalf("/healthz = %d, want 200", code)
	}
	if code, _ := get(t, srv, "/readyz"); code != http.StatusServiceUnavailable {
		t.Fatalf("/readyz before ready = %d, want 503", code)
	}
	ready.Store(true)
	if code, _ := get(t, srv, "/readyz"); code != http.StatusOK {
		t.Fatalf("/readyz when ready = %d, want 200", code)
	}

	m.Sweep("ok")
	m.Sweep("ok")
	m.Sweep("error")
	code, body := get(t, srv, "/metrics")
	if code != http.StatusOK {
		t.Fatalf("/metrics = %d", code)
	}
	for _, want := range []string{
		"vws_compute_ready 1",
		`vws_compute_reconcile_sweeps_total{result="ok"} 2`,
		`vws_compute_reconcile_sweeps_total{result="error"} 1`,
		"vws_compute_build_info{",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("/metrics lacks %q:\n%s", want, body)
		}
	}
	for _, line := range strings.Split(body, "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if !strings.HasPrefix(line, "vws_") {
			t.Errorf("metric without the vws_ prefix: %s", line)
		}
	}
}
