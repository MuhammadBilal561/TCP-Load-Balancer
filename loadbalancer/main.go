package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"net"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
)

type Backend struct {
	Address string
	Healthy bool
}

var (
	backends       []Backend
	currentBackend int

	// Protects backend state.
	mu sync.Mutex

	// Tracks active proxy connections.
	proxyWG sync.WaitGroup

	// Protects activeConnections.
	connMu sync.Mutex

	activeConnections = make(map[net.Conn]struct{})
)

func main() {
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
		fmt.Println("Error: at least one backend is required")
		return
	}

	// ---------------------------------------------------------
	// Shutdown context
	// ---------------------------------------------------------

	ctx, stop := signal.NotifyContext(
		context.Background(),
		osInterrupt(),
	)

	defer stop()

	// ---------------------------------------------------------
	// TCP listener
	// ---------------------------------------------------------

	listener, err := net.Listen("tcp", *listenAddr)
	if err != nil {
		fmt.Println("Error starting load balancer:", err)
		return
	}

	fmt.Println("Load balancer listening on", *listenAddr)

	fmt.Println("Configured backends:")

	for _, backend := range backends {
		fmt.Println(" -", backend.Address)
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
			// Listener was closed because shutdown started.
			if ctx.Err() != nil {
				break acceptLoop
			}

			fmt.Println("Error accepting client:", err)
			continue
		}

		backend, ok := getNextHealthyBackend()

		if !ok {
			fmt.Println("No healthy backends available")
			clientConn.Close()
			continue
		}

		fmt.Println("Routing client to:", backend)

		backendConn, err := net.DialTimeout(
			"tcp",
			backend,
			2*time.Second,
		)

		if err != nil {
			fmt.Println("Backend connection failed:", backend)

			markBackend(backend, false)

			clientConn.Close()

			continue
		}

		// Track both ends of this proxy connection.
		addConnection(clientConn)
		addConnection(backendConn)

		proxyWG.Add(1)

		go func() {
			defer proxyWG.Done()

			proxy(clientConn, backendConn)

			removeConnection(clientConn)
			removeConnection(backendConn)
		}()
	}

	// ---------------------------------------------------------
	// Begin shutdown
	// ---------------------------------------------------------

	fmt.Println("Shutdown signal received")

	// Stop accepting new clients.
	listener.Close()

	// Stop the health checker.
	stop()

	fmt.Println("Waiting for active proxy connections...")

	// Give existing connections time to finish.
	done := make(chan struct{})

	go func() {
		proxyWG.Wait()
		close(done)
	}()

	select {
	case <-done:
		fmt.Println("All proxy connections finished")

	case <-time.After(5 * time.Second):
		fmt.Println("Grace period expired")
		fmt.Println("Closing active connections")

		closeActiveConnections()

		proxyWG.Wait()
	}

	// Make sure health checker has stopped.
	healthWG.Wait()

	fmt.Println("Load balancer stopped")
}

func parseBackends(value string) []Backend {
	parts := strings.Split(value, ",")

	result := make([]Backend, 0, len(parts))

	for _, part := range parts {
		address := strings.TrimSpace(part)

		if address == "" {
			continue
		}

		result = append(result, Backend{
			Address: address,
			Healthy: true,
		})
	}

	return result
}

func getNextHealthyBackend() (string, bool) {
	mu.Lock()
	defer mu.Unlock()

	for i := 0; i < len(backends); i++ {
		index := (currentBackend + i) % len(backends)

		if backends[index].Healthy {
			currentBackend = (index + 1) % len(backends)

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

		if backends[i].Healthy == healthy {
			return
		}

		backends[i].Healthy = healthy

		if healthy {
			fmt.Println("Backend recovered:", address)
		} else {
			fmt.Println("Backend marked unhealthy:", address)
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
			fmt.Println("Health checker stopped")
			return

		case <-ticker.C:
			checkAllBackends()
		}
	}
}

func checkAllBackends() {
	// Copy addresses while holding the lock.
	mu.Lock()

	addresses := make([]string, 0, len(backends))

	for _, backend := range backends {
		addresses = append(addresses, backend.Address)
	}

	mu.Unlock()

	// Network operations happen outside the lock.
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

func proxy(client net.Conn, backend net.Conn) {
	defer client.Close()
	defer backend.Close()

	// Client -> Backend
	go io.Copy(backend, client)

	// Backend -> Client
	io.Copy(client, backend)
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

func osInterrupt() os.Signal {
	return syscall.SIGINT
}