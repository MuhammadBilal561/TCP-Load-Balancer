
package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
)

func main() {
	port := "9001"

	if len(os.Args) > 1 {
		port = os.Args[1]
	}

	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		fmt.Println("Error starting backend:", err)
		return
	}
	defer listener.Close()

	fmt.Println("Backend server listening on port", port)

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err)
			continue
		}

		// Each accepted connection is handled independently.
		go handleConnection(conn, port)
	}
}

func handleConnection(conn net.Conn, port string) {
	defer conn.Close()

	// This demo backend uses a newline as its message delimiter.
	reader := bufio.NewReader(conn)
	message, err := reader.ReadString('\n')
	if err != nil {
		// Health checks connect and close without sending a message.
		if !errors.Is(err, io.EOF) {
			fmt.Printf("Backend %s read error: %v\n", port, err)
		}
		return
	}

	fmt.Printf(
		"Backend %s received: %s\n",
		port,
		strings.TrimSpace(message),
	)

	response := []byte("Hello from BACKEND " + port)

	if _, err := conn.Write(response); err != nil {
		fmt.Printf("Backend %s write error: %v\n", port, err)
	}
}