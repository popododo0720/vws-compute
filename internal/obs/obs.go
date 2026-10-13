// Package obs serves the process's HTTP probes and Prometheus metrics:
//
//	/healthz  200 while the process runs; it checks no dependency, so a
//	          dependency outage does not restart the process
//	/readyz   200 while the readiness verdict (the one grpc.health.v1
//	          answers) is serving, 503 otherwise
//	/metrics  Prometheus text format; every metric is named vws_compute_*
package obs

import (
	"net/http"
	"runtime/debug"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics are the process's own metrics, in a registry of their own: no Go
// runtime or process collectors, so every exported name has the vws_ prefix.
type Metrics struct {
	reg    *prometheus.Registry
	sweeps *prometheus.CounterVec
}

// NewMetrics registers vws_compute_build_info, vws_compute_ready (from
// ready), and vws_compute_reconcile_sweeps_total.
func NewMetrics(ready func() bool) *Metrics {
	reg := prometheus.NewRegistry()
	version := "devel"
	if bi, ok := debug.ReadBuildInfo(); ok && bi.Main.Version != "" {
		version = bi.Main.Version
	}
	build := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "vws_compute_build_info", Help: "Always 1; the labels name the build.",
	}, []string{"version"})
	build.WithLabelValues(version).Set(1)
	readyGauge := prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "vws_compute_ready", Help: "1 while the readiness verdict is serving, else 0.",
	}, func() float64 {
		if ready() {
			return 1
		}
		return 0
	})
	sweeps := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "vws_compute_reconcile_sweeps_total", Help: "Reconcile sweeps by result: ok, waiting (machines still booting), or error.",
	}, []string{"result"})
	reg.MustRegister(build, readyGauge, sweeps)
	return &Metrics{reg: reg, sweeps: sweeps}
}

// Sweep counts one reconcile sweep with result. It is a
// reconciler.WithSweepObserver function.
func (m *Metrics) Sweep(result string) { m.sweeps.WithLabelValues(result).Inc() }

// Handler serves /healthz, /readyz, and /metrics.
func Handler(ready func() bool, m *Metrics) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		write(w, http.StatusOK, "ok\n")
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		if ready() {
			write(w, http.StatusOK, "ok\n")
			return
		}
		write(w, http.StatusServiceUnavailable, "not ready\n")
	})
	mux.Handle("GET /metrics", promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{}))
	return mux
}

func write(w http.ResponseWriter, code int, body string) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(code)
	_, _ = w.Write([]byte(body)) //nolint:errcheck // a failed write to a probe has no remedy
}
