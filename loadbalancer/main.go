package main

import (
	"fmt"
	"io"
	"net"
)

var backends = []string{
	"localhost:9001",
	"localhost:9002",
	"localhost:9003",
}

var currentBackend = 0

func main() {
	listener, err := net.Listen("tcp", ":8080")
	if err != nil {
		fmt.Println("Error starting load balancer:", err)
		return
	}
	defer listener.Close()

	fmt.Println("Load balancer listening on port 8080")

	for {
		clientConn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting client:", err)
			continue
		}

		backend := getNextBackend()

		fmt.Println("Routing client to:", backend)

		backendConn, err := net.Dial("tcp", backend)
		if err != nil {
			fmt.Println("Error connecting to backend:", err)
			clientConn.Close()
			continue
		}

		go proxy(clientConn, backendConn)
	}
}

func getNextBackend() string {
	backend := backends[currentBackend]

	currentBackend = (currentBackend + 1) % len(backends)

	return backend
}

func proxy(client net.Conn, backend net.Conn) {
	defer client.Close()
	defer backend.Close()

	go io.Copy(backend, client)

	io.Copy(client, backend)
}