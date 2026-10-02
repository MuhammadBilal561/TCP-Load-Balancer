package main

import (
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

type Backend struct {
	Address string
	Healthy bool
}

var (
	backends = []Backend{
		{Address: "localhost:9001", Healthy: true},
		{Address: "localhost:9002", Healthy: true},
		{Address: "localhost:9003", Healthy: true},
	}

	currentBackend = 0

	mu sync.Mutex
)

func main() {
	listener, err := net.Listen("tcp", ":8080")
	if err != nil {
		fmt.Println("Error starting load balancer:", err)
		return
	}
	defer listener.Close()

	fmt.Println("Load balancer listening on port 8080")

	go healthChecker()

	for {
		clientConn, err := listener.Accept()
		if err != nil {
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

		fmt.Println("Connected to backend:", backend)

		go proxy(clientConn, backendConn)
	}
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
		if backends[i].Address == address {
			if backends[i].Healthy != healthy {
				backends[i].Healthy = healthy

				if healthy {
					fmt.Println("Backend recovered:", address)
				} else {
					fmt.Println("Backend marked unhealthy:", address)
				}
			}

			return
		}
	}
}

func healthChecker() {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		for _, backend := range backends {
			conn, err := net.DialTimeout(
				"tcp",
				backend.Address,
				1*time.Second,
			)

			if err != nil {
				markBackend(backend.Address, false)
				continue
			}

			conn.Close()

			markBackend(backend.Address, true)
		}
	}
}

func proxy(client net.Conn, backend net.Conn) {
	defer client.Close()
	defer backend.Close()

	go io.Copy(backend, client)

	io.Copy(client, backend)
}