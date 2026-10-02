package main

import (
	"context"
	"flag"
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
	Address string
	Healthy bool
}

type copyResult struct {
	direction string
	bytes     int64
	err       error
}

var (
	backends       []Backend
	currentBackend int

	// Protects backend state.
	mu sync.Mutex

	// Tracks active TCP connections.
	connMu sync.Mutex

	activeConnections = make(map[net.Conn]struct{})

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
		"address for the load balancer to listen on",
	)

	backendList := flag.String(
		"backends",
		"localhost:9001,localhost:9002,localhost:9003",
		"comma-separated backend addresses",
	)

	flag.Parse()

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
	)

	logger.Info(
		"backends configured",
		"count", len(backends),
	)

	for _, backend := range backends {
		logger.Info(
			"backend configured",
			"address", backend.Address,
		)
	}

	// ---------------------------------------------------------
	// Health checker
	// ---------------------------------------------------------

	var healthWG sync.WaitGroup

	healthWG.Add(1)

	go func() {
		defer healthWG.Done()

		healthChecker(ctx)
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

			logger.Error(
				"failed to accept client",
				"error", err,
			)

			continue
		}

		// Give this connection a unique ID.
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

		backend, ok := getNextHealthyBackend()

		if !ok {
			logger.Error(
				"no healthy backends available",
				"connection_id", connectionID,
			)

			clientConn.Close()
			activeProxies.Add(-1)

			continue
		}

		logger.Info(
			"client routed",
			"connection_id", connectionID,
			"backend", backend,
		)

		// -----------------------------------------------------
		// Connect to backend
		// -----------------------------------------------------

		backendConn, err := net.DialTimeout(
			"tcp",
			backend,
			2*time.Second,
		)

		if err != nil {
			logger.Error(
				"failed to connect to backend",
				"connection_id", connectionID,
				"backend", backend,
				"error", err,
			)

			markBackend(backend, false)

			clientConn.Close()
			activeProxies.Add(-1)

			continue
		}

		logger.Info(
			"backend connected",
			"connection_id", connectionID,
			"backend", backend,
		)

		// Track sockets so shutdown can close them.
		addConnection(clientConn)
		addConnection(backendConn)

		proxyWG.Add(1)

		go func(
			id uint64,
			client net.Conn,
			backendConn net.Conn,
		) {
			defer proxyWG.Done()

			proxy(
				id,
				client,
				backendConn,
			)

			removeConnection(client)
			removeConnection(backendConn)

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
		)
	}

	// ---------------------------------------------------------
	// Shutdown
	// ---------------------------------------------------------

	logger.Info("shutdown signal received")

	listener.Close()

	stop()

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

	case <-time.After(5 * time.Second):
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

func parseBackends(value string) []Backend {
	parts := strings.Split(value, ",")

	result := make(
		[]Backend,
		0,
		len(parts),
	)

	for _, part := range parts {
		address := strings.TrimSpace(part)

		if address == "" {
			continue
		}

		result = append(
			result,
			Backend{
				Address: address,
				Healthy: true,
			},
		)
	}

	return result
}

func getNextHealthyBackend() (string, bool) {
	mu.Lock()
	defer mu.Unlock()

	for i := 0; i < len(backends); i++ {
		index := (currentBackend + i) % len(backends)

		if backends[index].Healthy {
			currentBackend =
				(index + 1) % len(backends)

			return backends[index].Address, true
		}
	}

	return "", false
}

func markBackend(address string, healthy bool) {
	mu.Lock()
	defer mu.Unlock()

	for i := range backends {
		if backends[i].Address != address {
			continue
		}

		// Nothing changed.
		if backends[i].Healthy == healthy {
			return
		}

		backends[i].Healthy = healthy

		if healthy {
			logger.Info(
				"backend recovered",
				"backend", address,
			)
		} else {
			logger.Warn(
				"backend marked unhealthy",
				"backend", address,
			)
		}

		return
	}
}

func healthChecker(ctx context.Context) {
	ticker := time.NewTicker(2 * time.Second)

	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():

			logger.Info(
				"health checker stopped",
			)

			return

		case <-ticker.C:

			checkAllBackends()
		}
	}
}

func checkAllBackends() {
	// Copy addresses while holding the lock.
	mu.Lock()

	addresses := make(
		[]string,
		0,
		len(backends),
	)

	for _, backend := range backends {
		addresses = append(
			addresses,
			backend.Address,
		)
	}

	mu.Unlock()

	// Network operations happen outside the mutex.
	for _, address := range addresses {

		conn, err := net.DialTimeout(
			"tcp",
			address,
			1*time.Second,
		)

		if err != nil {
			markBackend(address, false)
			continue
		}

		conn.Close()

		markBackend(address, true)
	}
}

func proxy(
	id uint64,
	client net.Conn,
	backend net.Conn,
) {
	start := time.Now()

	defer client.Close()
	defer backend.Close()

	results := make(
		chan copyResult,
		2,
	)

	// ---------------------------------------------------------
	// Client -> Backend
	// ---------------------------------------------------------

	go func() {
		n, err := io.Copy(
			backend,
			client,
		)

		results <- copyResult{
			direction: "client_to_backend",
			bytes:     n,
			err:       err,
		}
	}()

	// ---------------------------------------------------------
	// Backend -> Client
	// ---------------------------------------------------------

	go func() {
		n, err := io.Copy(
			client,
			backend,
		)

		results <- copyResult{
			direction: "backend_to_client",
			bytes:     n,
			err:       err,
		}
	}()

	// Wait for whichever direction finishes first.
	first := <-results

	// Closing both sockets causes the other io.Copy
	// to stop as well.
	client.Close()
	backend.Close()

	second := <-results

	var clientToBackend int64
	var backendToClient int64

	var proxyError error

	for _, result := range []copyResult{
		first,
		second,
	} {
		switch result.direction {

		case "client_to_backend":
			clientToBackend = result.bytes

		case "backend_to_client":
			backendToClient = result.bytes
		}

		if result.err != nil &&
			result.err != io.EOF {
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
	defer connMu.Unlock()

	for conn := range activeConnections {
		conn.Close()
	}
}

// WaitGroup tracks active proxy workers.
var proxyWG sync.WaitGroup