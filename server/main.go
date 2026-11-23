
package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
)

var (
	connectionMu sync.Mutex
	connections  = make(map[net.Conn]struct{})
)

func main() {
	port := "9001"

	if len(os.Args) > 1 {
		port = os.Args[1]
	}

	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("backend startup failed: %v", err)
	}

	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	log.Printf("backend listening on port %s", port)

	go func() {
		<-ctx.Done()
		_ = listener.Close()
	}()

	var workers sync.WaitGroup

	for {
		conn, err := listener.Accept()

		if err != nil {
			if ctx.Err() != nil {
				break
			}

			log.Printf("accept error: %v", err)
			continue
		}

		connectionMu.Lock()
		connections[conn] = struct{}{}
		connectionMu.Unlock()

		workers.Add(1)

		go func(conn net.Conn) {
			defer workers.Done()
			handleConnection(conn, port)
		}(conn)
	}

	stop()

	// Give active handlers up to five seconds to finish.
	done := make(chan struct{})

	go func() {
		workers.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Println("all backend connections finished")

	case <-time.After(5 * time.Second):
		log.Println("backend shutdown timeout; closing active connections")

		connectionMu.Lock()

		active := make([]net.Conn, 0, len(connections))
		for conn := range connections {
			active = append(active, conn)
		}

		connectionMu.Unlock()

		for _, conn := range active {
			_ = conn.Close()
		}

		workers.Wait()
	}

	log.Printf("backend %s stopped", port)
}

func handleConnection(conn net.Conn, port string) {
	defer conn.Close()

	defer func() {
		connectionMu.Lock()
		delete(connections, conn)
		connectionMu.Unlock()
	}()

	// Prevent an incomplete demo request from hanging forever.
	_ = conn.SetReadDeadline(time.Now().Add(30 * time.Second))

	reader := bufio.NewReader(conn)

	message, err := reader.ReadString('\n')

	if err != nil {
		if !errors.Is(err, io.EOF) &&
			!errors.Is(err, os.ErrDeadlineExceeded) {
			log.Printf("backend %s read error: %v", port, err)
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

	response := fmt.Sprintf("Hello from BACKEND %s", port)

	if _, err := io.WriteString(conn, response); err != nil {
		log.Printf("backend %s write error: %v", port, err)
	}
}
