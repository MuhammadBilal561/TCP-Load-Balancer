
package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"
	"sync"
)

func main() {
	port := "9001"

	if len(os.Args) > 1 {
		port = os.Args[1]
	}

	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("failed to start backend on port %s: %v", port, err)
	}

	defer listener.Close()

	log.Printf("backend listening on port %s", port)

	var workers sync.WaitGroup

	defer workers.Wait()

	for {
		conn, err := listener.Accept()

		if err != nil {
			log.Printf("accept error on port %s: %v", port, err)
			continue
		}

		workers.Add(1)

		go func(conn net.Conn) {
			defer workers.Done()
			handleConnection(conn, port)
		}(conn)
	}
}

func handleConnection(conn net.Conn, port string) {
	defer conn.Close()

	reader := bufio.NewReader(conn)

	message, err := reader.ReadString('\n')

	if err != nil {
		// Health checks connect and immediately close.
		if !errors.Is(err, io.EOF) {
			log.Printf("read error on backend %s: %v", port, err)
		}
		return
	}

	message = strings.TrimSpace(message)

	log.Printf(
		"backend=%s received=%q remote=%s",
		port,
		message,
		conn.RemoteAddr(),
	)

	response := fmt.Sprintf(
		"Hello from BACKEND %s",
		port,
	)

	if _, err := io.WriteString(conn, response); err != nil {
		log.Printf("write error on backend %s: %v", port, err)
		return
	}
}
