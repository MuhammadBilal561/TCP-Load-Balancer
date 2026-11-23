
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type Backend struct {
	Address              string
	Healthy              bool
	ConsecutiveFailures  int
	ConsecutiveSuccesses int
	ActiveConnections    int64
}

type copyResult struct {
	direction string
	bytes     int64
	err       error
}

var (
	backends       []Backend
	currentBackend int

	mu sync.Mutex

	connMu            sync.Mutex
	activeConnections = make(map[net.Conn]struct{})

	proxyWG sync.WaitGroup

	nextConnectionID atomic.Uint64
	totalConnections  atomic.Uint64
	activeProxies     atomic.Int64

	backendDialSuccesses atomic.Uint64
	backendDialFailures  atomic.Uint64

	bytesClientToBackend atomic.Uint64
	bytesBackendToClient atomic.Uint64

	proxyErrors atomic.Uint64

	logger           *slog.Logger
	processStartedAt time.Time
)

func main() {
	processStartedAt = time.Now()

	logger = slog.New(
		slog.NewTextHandler(
			os.Stdout,
			&slog.HandlerOptions{Level: slog.LevelInfo},
		),
	)

	// Configuration can come from command-line flags or environment.
	listenAddr := flag.String(
		"listen",
		envOrDefault("LB_LISTEN", ":8080"),
		"TCP listener address",
	)

	metricsAddr := flag.String(
		"metrics",
		envOrDefault("LB_METRICS_LISTEN", ":9090"),
		"HTTP metrics listener address; empty disables metrics",
	)

	backendList := flag.String(
		"backends",
		envOrDefault(
			"LB_BACKENDS",
			"localhost:9001,localhost:9002,localhost:9003",
		),
		"comma-separated backend addresses",
	)

	algorithm := flag.String(
		"algorithm",
		envOrDefault("LB_ALGORITHM", "round-robin"),
		"round-robin or least-connections",
	)

	healthInterval := flag.Duration(
		"health-interval", 2*time.Second,
		"interval between health checks",
	)

	healthTimeout := flag.Duration(
		"health-timeout", time.Second,
		"health check timeout",
	)

	failureThreshold := flag.Int(
		"health-failure-threshold", 2,
		"consecutive failures before marking unhealthy",
	)

	successThreshold := flag.Int(
		"health-success-threshold", 2,
		"consecutive successes before recovery",
	)

	shutdownTimeout := flag.Duration(
		"shutdown-timeout", 10*time.Second,
		"graceful shutdown timeout",
	)

	flag.Parse()

	if *algorithm != "round-robin" &&
		*algorithm != "least-connections" {
		logger.Error("unsupported algorithm", "algorithm", *algorithm)
		return
	}

	if *healthInterval <= 0 ||
		*healthTimeout <= 0 ||
		*shutdownTimeout <= 0 ||
		*failureThreshold < 1 ||
		*successThreshold < 1 {
		logger.Error("invalid configuration values")
		return
	}

	backends = parseBackends(*backendList)

	if len(backends) == 0 {
		logger.Error("no backends configured")
		return
	}

	// Kubernetes sends SIGTERM during pod termination.
	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	listener, err := net.Listen("tcp", *listenAddr)
	if err != nil {
		logger.Error(
			"failed to start TCP listener",
			"address", *listenAddr,
			"error", err,
		)
		return
	}

	logger.Info(
		"load balancer started",
		"listen", *listenAddr,
		"algorithm", *algorithm,
	)

	for _, backend := range backends {
		logger.Info("backend configured", "address", backend.Address)
	}

	// Stop Accept() when the process receives a shutdown signal.
	go func() {
		<-ctx.Done()
		_ = listener.Close()
	}()

	// Start the separate monitoring server.
	var metricsServer *http.Server

	if *metricsAddr != "" {
		metricsServer = &http.Server{
			Addr:              *metricsAddr,
			Handler:           newMetricsHandler(),
			ReadHeaderTimeout: 5 * time.Second,
		}

		go func() {
			logger.Info(
				"metrics server started",
				"address", *metricsAddr,
			)

			err := metricsServer.ListenAndServe()

			if err != nil && err != http.ErrServerClosed {
				logger.Error("metrics server failed", "error", err)
			}
		}()
	}

	// Start background health monitoring.
	var healthWG sync.WaitGroup
	healthWG.Add(1)

	go func() {
		defer healthWG.Done()

		healthChecker(
			ctx,
			*healthInterval,
			*healthTimeout,
			*failureThreshold,
			*successThreshold,
		)
	}()

	// Main TCP accept loop.
acceptLoop:
	for {
		clientConn, err := listener.Accept()

		if err != nil {
			if ctx.Err() != nil {
				break acceptLoop
			}

			logger.Error("failed to accept client", "error", err)
			continue
		}

		if ctx.Err() != nil {
			_ = clientConn.Close()
			break acceptLoop
		}

		id := nextConnectionID.Add(1)
		totalConnections.Add(1)
		activeProxies.Add(1)

		logger.Info(
			"client connected",
			"connection_id", id,
			"remote", clientConn.RemoteAddr().String(),
			"active_connections", activeProxies.Load(),
		)

		backend, ok := selectBackend(*algorithm)

		if !ok {
			logger.Error("no healthy backends available",
				"connection_id", id)

			_ = clientConn.Close()
			activeProxies.Add(-1)
			continue
		}

		dialer := net.Dialer{Timeout: 2 * time.Second}

		backendConn, err := dialer.DialContext(
			ctx,
			"tcp",
			backend,
		)

		if err != nil {
			backendDialFailures.Add(1)
			releaseBackend(backend)

			_ = clientConn.Close()
			activeProxies.Add(-1)

			if ctx.Err() != nil {
				break acceptLoop
			}

			logger.Error(
				"backend connection failed",
				"connection_id", id,
				"backend", backend,
				"error", err,
			)

			recordBackendHealth(
				backend,
				false,
				*failureThreshold,
				*successThreshold,
			)
			continue
		}

		if ctx.Err() != nil {
			_ = clientConn.Close()
			_ = backendConn.Close()

			releaseBackend(backend)
			activeProxies.Add(-1)
			break acceptLoop
		}

		backendDialSuccesses.Add(1)

		addConnection(clientConn)
		addConnection(backendConn)

		logger.Info(
			"backend connected",
			"connection_id", id,
			"backend", backend,
		)

		proxyWG.Add(1)

		go func(
			id uint64,
			client net.Conn,
			server net.Conn,
			address string,
		) {
			defer proxyWG.Done()

			proxy(id, client, server)

			removeConnection(client)
			removeConnection(server)

			releaseBackend(address)
			activeProxies.Add(-1)

			logger.Info(
				"proxy finished",
				"connection_id", id,
				"active_connections", activeProxies.Load(),
			)
		}(
			id,
			clientConn,
			backendConn,
			backend,
		)
	}

	// Graceful shutdown.
	logger.Info("shutdown signal received")

	stop()
	_ = listener.Close()

	done := make(chan struct{})

	go func() {
		proxyWG.Wait()
		close(done)
	}()

	select {
	case <-done:
		logger.Info("all proxy connections finished")

	case <-time.After(*shutdownTimeout):
		logger.Warn(
			"shutdown timeout reached",
			"active_connections", activeProxies.Load(),
		)

		closeActiveConnections()
		proxyWG.Wait()
	}

	healthWG.Wait()

	if metricsServer != nil {
		shutdownCtx, cancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)

		if err := metricsServer.Shutdown(shutdownCtx); err != nil {
			logger.Error("metrics shutdown failed", "error", err)
			_ = metricsServer.Close()
		}

		cancel()
	}

	logger.Info(
		"load balancer stopped",
		"total_connections", totalConnections.Load(),
	)
}

func envOrDefault(key, fallback string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	return value
}

func parseBackends(value string) []Backend {
	parts := strings.Split(value, ",")
	result := make([]Backend, 0, len(parts))
	seen := make(map[string]bool)

	for _, part := range parts {
		address := strings.TrimSpace(part)

		if address == "" || seen[address] {
			continue
		}

		seen[address] = true

		result = append(result, Backend{
			Address: address,
			Healthy: true,
		})
	}

	return result
}

func selectBackend(algorithm string) (string, bool) {
	mu.Lock()
	defer mu.Unlock()

	if len(backends) == 0 {
		return "", false
	}

	selected := -1

	switch algorithm {
	case "round-robin":
		for i := 0; i < len(backends); i++ {
			index := (currentBackend + i) % len(backends)

			if backends[index].Healthy {
				selected = index
				break
			}
		}

	case "least-connections":
		for i := 0; i < len(backends); i++ {
			index := (currentBackend + i) % len(backends)

			if !backends[index].Healthy {
				continue
			}

			if selected == -1 ||
				backends[index].ActiveConnections <
					backends[selected].ActiveConnections {
				selected = index
			}
		}

	default:
		return "", false
	}

	if selected == -1 {
		return "", false
	}

	currentBackend = (selected + 1) % len(backends)
	backends[selected].ActiveConnections++

	return backends[selected].Address, true
}

func getNextHealthyBackend() (string, bool) {
	return selectBackend("round-robin")
}

func releaseBackend(address string) {
	mu.Lock()
	defer mu.Unlock()

	for i := range backends {
		if backends[i].Address == address {
			if backends[i].ActiveConnections > 0 {
				backends[i].ActiveConnections--
			}
			return
		}
	}
}

func recordBackendHealth(
	address string,
	success bool,
	failureThreshold int,
	successThreshold int,
) {
	var transition string

	mu.Lock()

	for i := range backends {
		backend := &backends[i]

		if backend.Address != address {
			continue
		}

		if success {
			backend.ConsecutiveFailures = 0

			if !backend.Healthy {
				backend.ConsecutiveSuccesses++

				if backend.ConsecutiveSuccesses >= successThreshold {
					backend.Healthy = true
					backend.ConsecutiveSuccesses = 0
					transition = "recovered"
				}
			} else {
				backend.ConsecutiveSuccesses = 0
			}
		} else {
			backend.ConsecutiveSuccesses = 0

			if backend.Healthy {
				backend.ConsecutiveFailures++

				if backend.ConsecutiveFailures >= failureThreshold {
					backend.Healthy = false
					backend.ConsecutiveFailures = 0
					transition = "unhealthy"
				}
			} else {
				backend.ConsecutiveFailures = 0
			}
		}

		break
	}

	mu.Unlock()

	if transition == "recovered" {
		logger.Info("backend recovered", "backend", address)
	} else if transition == "unhealthy" {
		logger.Warn("backend unhealthy", "backend", address)
	}
}

func markBackend(address string, healthy bool) {
	var changed bool

	mu.Lock()

	for i := range backends {
		if backends[i].Address == address {
			changed = backends[i].Healthy != healthy
			backends[i].Healthy = healthy
			backends[i].ConsecutiveFailures = 0
			backends[i].ConsecutiveSuccesses = 0
			break
		}
	}

	mu.Unlock()

	if changed && logger != nil {
		if healthy {
			logger.Info("backend recovered", "backend", address)
		} else {
			logger.Warn("backend unhealthy", "backend", address)
		}
	}
}

func healthChecker(
	ctx context.Context,
	interval time.Duration,
	timeout time.Duration,
	failureThreshold int,
	successThreshold int,
) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logger.Info("health checker stopped")
			return

		case <-ticker.C:
			checkAllBackends(
				ctx,
				timeout,
				failureThreshold,
				successThreshold,
			)
		}
	}
}

func checkAllBackends(
	ctx context.Context,
	timeout time.Duration,
	failureThreshold int,
	successThreshold int,
) {
	mu.Lock()

	addresses := make([]string, 0, len(backends))
	for _, backend := range backends {
		addresses = append(addresses, backend.Address)
	}

	mu.Unlock()

	var wg sync.WaitGroup

	for _, address := range addresses {
		wg.Add(1)

		go func(address string) {
			defer wg.Done()

			checkCtx, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()

			dialer := net.Dialer{}

			conn, err := dialer.DialContext(
				checkCtx,
				"tcp",
				address,
			)

			if err != nil {
				if ctx.Err() == nil {
					recordBackendHealth(
						address,
						false,
						failureThreshold,
						successThreshold,
					)
				}
				return
			}

			_ = conn.Close()

			recordBackendHealth(
				address,
				true,
				failureThreshold,
				successThreshold,
			)
		}(address)
	}

	wg.Wait()
}

func proxy(id uint64, client, backend net.Conn) {
	start := time.Now()

	defer client.Close()
	defer backend.Close()

	results := make(chan copyResult, 2)

	go func() {
		n, err := io.Copy(backend, client)

		if err == nil {
			err = closeWrite(backend)
		}

		results <- copyResult{
			direction: "client_to_backend",
			bytes: n,
			err: err,
		}
	}()

	go func() {
		n, err := io.Copy(client, backend)

		if err == nil {
			err = closeWrite(client)
		}

		results <- copyResult{
			direction: "backend_to_client",
			bytes: n,
			err: err,
		}
	}()

	first := <-results

	if first.err != nil {
		_ = client.Close()
		_ = backend.Close()
	}

	second := <-results

	var clientBytes, backendBytes int64
	var proxyError error

	for _, result := range []copyResult{first, second} {
		switch result.direction {
		case "client_to_backend":
			clientBytes = result.bytes
		case "backend_to_client":
			backendBytes = result.bytes
		}

		if result.err != nil && proxyError == nil {
			proxyError = result.err
		}
	}

	bytesClientToBackend.Add(uint64(clientBytes))
	bytesBackendToClient.Add(uint64(backendBytes))

	if proxyError != nil {
		proxyErrors.Add(1)

		logger.Warn(
			"proxy ended with error",
			"connection_id", id,
			"duration", time.Since(start),
			"error", proxyError,
		)
		return
	}

	logger.Info(
		"connection closed",
		"connection_id", id,
		"duration", time.Since(start),
		"client_to_backend_bytes", clientBytes,
		"backend_to_client_bytes", backendBytes,
	)
}

func closeWrite(conn net.Conn) error {
	tcpConn, ok := conn.(*net.TCPConn)
	if !ok {
		return fmt.Errorf("unsupported TCP connection type %T", conn)
	}
	return tcpConn.CloseWrite()
}

func addConnection(conn net.Conn) {
	connMu.Lock()
	defer connMu.Unlock()
	activeConnections[conn] = struct{}{}
}

func removeConnection(conn net.Conn) {
	connMu.Lock()
	defer connMu.Unlock()
	delete(activeConnections, conn)
}

func closeActiveConnections() {
	connMu.Lock()

	connections := make([]net.Conn, 0, len(activeConnections))
	for conn := range activeConnections {
		connections = append(connections, conn)
	}

	connMu.Unlock()

	for _, conn := range connections {
		_ = conn.Close()
	}
}
