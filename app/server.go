package main

import (
	"fmt"
	"net"
)

func main() {
	initMeta()

	port := _metaInfo.port
	// Listen for incoming connections
	listener, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", port))
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	// Ensure we teardown the server when the program exits
	defer listener.Close()

	fmt.Println(fmt.Sprintf("Server is listening on port %d", port))

	if !_metaInfo.isMaster() {
		handshake()
	}

	for {
		// Block until we receive an incoming connection
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error:", err)
			continue
		}

		// Handle client connection
		go handleClient(conn)
	}
}
