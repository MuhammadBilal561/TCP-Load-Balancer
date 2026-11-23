
package main

import (
	"fmt"
	"net/http"
	"strings"
	"time"
)

func newMetricsHandler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/metrics", serveMetrics)
	mux.HandleFunc("/livez", serveLiveness)
	mux.HandleFunc("/readyz", serveReadiness)

	return mux
}

// Liveness means the process is running.
func serveLiveness(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("alive\n"))
}

// Readiness means at least one backend is available.
func serveReadiness(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	mu.Lock()

	ready := false

	for _, backend := range backends {
		if backend.Healthy {
			ready = true
			break
		}
	}

	mu.Unlock()

	if !ready {
		http.Error(
			w,
			"no healthy backends available",
			http.StatusServiceUnavailable,
		)
		return
	}

	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ready\n"))
}

func serveMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set(
		"Content-Type",
		"text/plain; version=0.0.4; charset=utf-8",
	)

	type backendSnapshot struct {
		address    string
		healthy    bool
		active     int64
		failures   int
		successes  int
	}

	mu.Lock()

	snapshots := make([]backendSnapshot, 0, len(backends))

	for _, backend := range backends {
		snapshots = append(snapshots, backendSnapshot{
			address:   backend.Address,
			healthy:   backend.Healthy,
			active:    backend.ActiveConnections,
			failures:  backend.ConsecutiveFailures,
			successes: backend.ConsecutiveSuccesses,
		})
	}

	mu.Unlock()

	uptime := time.Since(processStartedAt).Seconds()
	if uptime < 0 {
		uptime = 0
	}

	// Process metrics.
	fmt.Fprintln(w, "# HELP loadbalancer_uptime_seconds Process uptime.")
	fmt.Fprintln(w, "# TYPE loadbalancer_uptime_seconds gauge")
	fmt.Fprintf(w, "loadbalancer_uptime_seconds %.2f\n", uptime)

	fmt.Fprintln(w, "# HELP loadbalancer_connections_accepted_total Total client connections.")
	fmt.Fprintln(w, "# TYPE loadbalancer_connections_accepted_total counter")
	fmt.Fprintf(w, "loadbalancer_connections_accepted_total %d\n", totalConnections.Load())

	fmt.Fprintln(w, "# HELP loadbalancer_connections_active Current active proxy connections.")
	fmt.Fprintln(w, "# TYPE loadbalancer_connections_active gauge")
	fmt.Fprintf(w, "loadbalancer_connections_active %d\n", activeProxies.Load())

	// Backend dial metrics.
	fmt.Fprintln(w, "# HELP loadbalancer_backend_connections_successful_total Successful backend dials.")
	fmt.Fprintln(w, "# TYPE loadbalancer_backend_connections_successful_total counter")
	fmt.Fprintf(w, "loadbalancer_backend_connections_successful_total %d\n", backendDialSuccesses.Load())

	fmt.Fprintln(w, "# HELP loadbalancer_backend_connections_failed_total Failed backend dials.")
	fmt.Fprintln(w, "# TYPE loadbalancer_backend_connections_failed_total counter")
	fmt.Fprintf(w, "loadbalancer_backend_connections_failed_total %d\n", backendDialFailures.Load())

	// Traffic metrics.
	fmt.Fprintln(w, "# HELP loadbalancer_bytes_client_to_backend_total Bytes forwarded client to backend.")
	fmt.Fprintln(w, "# TYPE loadbalancer_bytes_client_to_backend_total counter")
	fmt.Fprintf(w, "loadbalancer_bytes_client_to_backend_total %d\n", bytesClientToBackend.Load())

	fmt.Fprintln(w, "# HELP loadbalancer_bytes_backend_to_client_total Bytes forwarded backend to client.")
	fmt.Fprintln(w, "# TYPE loadbalancer_bytes_backend_to_client_total counter")
	fmt.Fprintf(w, "loadbalancer_bytes_backend_to_client_total %d\n", bytesBackendToClient.Load())

	// Proxy error metrics.
	fmt.Fprintln(w, "# HELP loadbalancer_proxy_errors_total Total proxy I/O errors.")
	fmt.Fprintln(w, "# TYPE loadbalancer_proxy_errors_total counter")
	fmt.Fprintf(w, "loadbalancer_proxy_errors_total %d\n", proxyErrors.Load())

	// Per-backend metrics.
	fmt.Fprintln(w, "# HELP loadbalancer_backend_healthy Backend health status, 1 healthy and 0 unhealthy.")
	fmt.Fprintln(w, "# TYPE loadbalancer_backend_healthy gauge")

	fmt.Fprintln(w, "# HELP loadbalancer_backend_active_connections Active connections assigned to backend.")
	fmt.Fprintln(w, "# TYPE loadbalancer_backend_active_connections gauge")

	fmt.Fprintln(w, "# HELP loadbalancer_backend_consecutive_failures Current consecutive health failures.")
	fmt.Fprintln(w, "# TYPE loadbalancer_backend_consecutive_failures gauge")

	fmt.Fprintln(w, "# HELP loadbalancer_backend_consecutive_successes Current consecutive health successes.")
	fmt.Fprintln(w, "# TYPE loadbalancer_backend_consecutive_successes gauge")

	for _, backend := range snapshots {
		labels := fmt.Sprintf(
			`backend="%s"`,
			escapeMetricLabel(backend.address),
		)

		healthy := 0
		if backend.healthy {
			healthy = 1
		}

		fmt.Fprintf(
			w,
			"loadbalancer_backend_healthy{%s} %d\n",
			labels,
			healthy,
		)

		fmt.Fprintf(
			w,
			"loadbalancer_backend_active_connections{%s} %d\n",
			labels,
			backend.active,
		)

		fmt.Fprintf(
			w,
			"loadbalancer_backend_consecutive_failures{%s} %d\n",
			labels,
			backend.failures,
		)

		fmt.Fprintf(
			w,
			"loadbalancer_backend_consecutive_successes{%s} %d\n",
			labels,
			backend.successes,
		)
	}
}

func escapeMetricLabel(value string) string {
	replacer := strings.NewReplacer(
		"\\", "\\\\",
		"\n", "\\n",
		"\"", "\\\"",
	)

	return replacer.Replace(value)
}
