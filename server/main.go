package main

import (
	"fmt"
	"net"
	"os"
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

		fmt.Println("Client connected:", conn.RemoteAddr())

		buffer := make([]byte, 1024)

		n, err := conn.Read(buffer)
		if err != nil {
			fmt.Println("Error reading:", err)
			conn.Close()
			continue
		}

		fmt.Println("Backend", port, "received:", string(buffer[:n]))

		response := []byte("Hello from BACKEND " + port)

		_, err = conn.Write(response)
		if err != nil {
			fmt.Println("Error writing:", err)
		}

		conn.Close()
	}
}