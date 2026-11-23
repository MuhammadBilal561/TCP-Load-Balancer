
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
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

	// Protects all backend state.
	mu sync.Mutex

	// Protects active TCP connections.
	connMu sync.Mutex
	activeConnections = make(map[net.Conn]struct{})

	// Tracks proxy goroutines.
	proxyWG sync.WaitGroup

	// Connection statistics.
	nextConnectionID atomic.Uint64
	totalConnections  atomic.Uint64
	activeProxies     atomic.Int64

	// Structured logger.
	logger *slog.Logger
)

func main() {
	// ---------------------------------------------------------
	// Logger
	// ---------------------------------------------------------

	logger = slog.New(
		slog.NewTextHandler(
			os.Stdout,
			&slog.HandlerOptions{
				Level: slog.LevelInfo,
			},
		),
	)

	// ---------------------------------------------------------
	// Configuration
	// ---------------------------------------------------------

	listenAddr := flag.String(
		"listen",
		":8080",
		"address for the load balancer",
	)

	backendList := flag.String(
		"backends",
		"localhost:9001,localhost:9002,localhost:9003",
		"comma-separated backend addresses",
	)

	algorithm := flag.String(
		"algorithm",
		"round-robin",
		"backend selection algorithm: round-robin or least-connections",
	)

	healthInterval := flag.Duration(
		"health-interval",
		2*time.Second,
		"interval between health checks",
	)

	healthTimeout := flag.Duration(
		"health-timeout",
		time.Second,
		"timeout for individual health checks",
	)

	failureThreshold := flag.Int(
		"health-failure-threshold",
		2,
		"consecutive failures before marking a backend unhealthy",
	)

	successThreshold := flag.Int(
		"health-success-threshold",
		2,
		"consecutive successes before recovering a backend",
	)

	shutdownTimeout := flag.Duration(
		"shutdown-timeout",
		5*time.Second,
		"maximum graceful shutdown duration",
	)

	flag.Parse()

	// ---------------------------------------------------------
	// Validate configuration
	// ---------------------------------------------------------

	if *algorithm != "round-robin" &&
		*algorithm != "least-connections" {
		logger.Error("unsupported balancing algorithm", "algorithm", *algorithm)
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

	// ---------------------------------------------------------
	// Shutdown context
	// ---------------------------------------------------------

	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
	)

	defer stop()

	// ---------------------------------------------------------
	// TCP listener
	// ---------------------------------------------------------

	listener, err := net.Listen("tcp", *listenAddr)
	if err != nil {
		logger.Error(
			"failed to start listener",
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
		logger.Info(
			"backend configured",
			"address", backend.Address,
		)
	}

	// Closing the listener on cancellation unblocks Accept().
	go func() {
		<-ctx.Done()
		_ = listener.Close()
	}()

	// ---------------------------------------------------------
	// Health checker
	// ---------------------------------------------------------

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

	// ---------------------------------------------------------
	// Accept loop
	// ---------------------------------------------------------

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

		connectionID := nextConnectionID.Add(1)

		totalConnections.Add(1)
		activeProxies.Add(1)

		logger.Info(
			"client connected",
			"connection_id", connectionID,
			"remote", clientConn.RemoteAddr().String(),
			"active_connections", activeProxies.Load(),
		)

		// -----------------------------------------------------
		// Select backend
		// -----------------------------------------------------

		backend, ok := selectBackend(*algorithm)

		if !ok {
			logger.Error(
				"no healthy backends available",
				"connection_id", connectionID,
			)

			_ = clientConn.Close()
			activeProxies.Add(-1)
			continue
		}

		logger.Info(
			"client routed",
			"connection_id", connectionID,
			"backend", backend,
			"algorithm", *algorithm,
		)

		// -----------------------------------------------------
		// Connect to backend
		// -----------------------------------------------------

		dialer := net.Dialer{
			Timeout: 2 * time.Second,
		}

		backendConn, err := dialer.DialContext(
			ctx,
			"tcp",
			backend,
		)

		if err != nil {
			releaseBackend(backend)

			_ = clientConn.Close()
			activeProxies.Add(-1)

			if ctx.Err() != nil {
				break acceptLoop
			}

			logger.Error(
				"backend connection failed",
				"connection_id", connectionID,
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

		logger.Info(
			"backend connected",
			"connection_id", connectionID,
			"backend", backend,
		)

		// -----------------------------------------------------
		// Track active connections
		// -----------------------------------------------------

		addConnection(clientConn)
		addConnection(backendConn)

		proxyWG.Add(1)

		go func(
			id uint64,
			client net.Conn,
			backendConn net.Conn,
			backendAddress string,
		) {
			defer proxyWG.Done()

			proxy(id, client, backendConn)

			removeConnection(client)
			removeConnection(backendConn)

			releaseBackend(backendAddress)

			activeProxies.Add(-1)

			logger.Info(
				"proxy finished",
				"connection_id", id,
				"active_connections", activeProxies.Load(),
			)

		}(
			connectionID,
			clientConn,
			backendConn,
			backend,
		)
	}

	// ---------------------------------------------------------
	// Graceful shutdown
	// ---------------------------------------------------------

	logger.Info("shutdown signal received")

	stop()
	_ = listener.Close()

	logger.Info(
		"waiting for active connections",
		"active_connections", activeProxies.Load(),
	)

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

	logger.Info(
		"load balancer stopped",
		"total_connections", totalConnections.Load(),
	)
}

// -------------------------------------------------------------
// Parse backend configuration
// -------------------------------------------------------------

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

// -------------------------------------------------------------
// Backend selection
// -------------------------------------------------------------

func selectBackend(algorithm string) (string, bool) {
	mu.Lock()
	defer mu.Unlock()

	if len(backends) == 0 {
		return "", false
	}

	selectedIndex := -1

	switch algorithm {

	case "round-robin":

		for i := 0; i < len(backends); i++ {
			index := (currentBackend + i) % len(backends)

			if backends[index].Healthy {
				selectedIndex = index
				break
			}
		}

	case "least-connections":

		for i := 0; i < len(backends); i++ {
			index := (currentBackend + i) % len(backends)

			if !backends[index].Healthy {
				continue
			}

			if selectedIndex == -1 ||
				backends[index].ActiveConnections <
					backends[selectedIndex].ActiveConnections {
				selectedIndex = index
			}
		}

	default:
		return "", false
	}

	if selectedIndex == -1 {
		return "", false
	}

	// Advance the starting point to rotate ties fairly.
	currentBackend = (selectedIndex + 1) % len(backends)

	// Reserve one active connection for this backend.
	backends[selectedIndex].ActiveConnections++

	return backends[selectedIndex].Address, true
}

// Maintains compatibility with the earlier Round Robin tests.
func getNextHealthyBackend() (string, bool) {
	return selectBackend("round-robin")
}

// -------------------------------------------------------------
// Release backend connection reservation
// -------------------------------------------------------------

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

// -------------------------------------------------------------
// Backend health state
// -------------------------------------------------------------

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

			if backend.Healthy {
				backend.ConsecutiveSuccesses = 0
			} else {
				backend.ConsecutiveSuccesses++

				if backend.ConsecutiveSuccesses >= successThreshold {
					backend.Healthy = true
					transition = "recovered"

					backend.ConsecutiveSuccesses = 0
					backend.ConsecutiveFailures = 0
				}
			}
		} else {
			backend.ConsecutiveSuccesses = 0

			if !backend.Healthy {
				backend.ConsecutiveFailures = 0
			} else {
				backend.ConsecutiveFailures++

				if backend.ConsecutiveFailures >= failureThreshold {
					backend.Healthy = false
					transition = "unhealthy"

					backend.ConsecutiveFailures = 0
					backend.ConsecutiveSuccesses = 0
				}
			}
		}

		break
	}

	mu.Unlock()

	if transition == "recovered" {
		logger.Info("backend recovered", "backend", address)
	} else if transition == "unhealthy" {
		logger.Warn("backend marked unhealthy", "backend", address)
	}
}

// Retained for compatibility with earlier unit tests.
// Production health decisions use recordBackendHealth().
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
			logger.Warn("backend marked unhealthy", "backend", address)
		}
	}
}

// -------------------------------------------------------------
// Health checker
// -------------------------------------------------------------

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

	// Check backends concurrently.
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
				// Avoid marking everything unhealthy during shutdown.
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

// -------------------------------------------------------------
// TCP bidirectional proxy with half-close support
// -------------------------------------------------------------

func proxy(
	id uint64,
	client net.Conn,
	backend net.Conn,
) {
	start := time.Now()

	defer client.Close()
	defer backend.Close()

	results := make(chan copyResult, 2)

	// Client -> Backend
	go func() {
		n, err := io.Copy(backend, client)

		if err == nil {
			err = closeWrite(backend)
		}

		results <- copyResult{
			direction: "client_to_backend",
			bytes:     n,
			err:       err,
		}
	}()

	// Backend -> Client
	go func() {
		n, err := io.Copy(client, backend)

		if err == nil {
			err = closeWrite(client)
		}

		results <- copyResult{
			direction: "backend_to_client",
			bytes:     n,
			err:       err,
		}
	}()

	// Wait for the first direction to finish.
	first := <-results

	if first.err != nil {
		_ = client.Close()
		_ = backend.Close()
	}

	// Wait for the second direction.
	second := <-results

	var clientToBackend int64
	var backendToClient int64
	var proxyError error

	for _, result := range []copyResult{first, second} {
		switch result.direction {
		case "client_to_backend":
			clientToBackend = result.bytes

		case "backend_to_client":
			backendToClient = result.bytes
		}

		if result.err != nil && proxyError == nil {
			proxyError = result.err
		}
	}

	duration := time.Since(start)

	if proxyError != nil {
		logger.Warn(
			"proxy ended with error",
			"connection_id", id,
			"duration", duration,
			"client_to_backend_bytes", clientToBackend,
			"backend_to_client_bytes", backendToClient,
			"error", proxyError,
		)

		return
	}

	logger.Info(
		"connection closed",
		"connection_id", id,
		"duration", duration,
		"client_to_backend_bytes", clientToBackend,
		"backend_to_client_bytes", backendToClient,
	)
}

// TCP half-close: finish sending without closing receiving.
func closeWrite(conn net.Conn) error {
	tcpConn, ok := conn.(*net.TCPConn)

	if !ok {
		return fmt.Errorf(
			"TCP half-close unsupported for connection type %T",
			conn,
		)
	}

	return tcpConn.CloseWrite()
}

// -------------------------------------------------------------
// Active connection tracking
// -------------------------------------------------------------

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
