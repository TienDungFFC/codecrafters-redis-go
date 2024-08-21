package main

import (
	"fmt"
	"net"
	"os"
)

type Handler struct {
	conn net.Conn
}

func NewHandler(c net.Conn) *Handler {
	return &Handler{
		conn: c,
	}
}
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
		conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", _metaInfo.masterHost, _metaInfo.masterPort))
		if err != nil {
			fmt.Printf("failed to dial master")
			os.Exit(-1)
		}
		cHandler := NewHandler(conn)
		cHandler.handshake()
	}

	for {
		// Block until we receive an incoming connection
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error:", err)
			continue
		}

		h := NewHandler(conn)

		// Handle client connection
		go h.handleClient()
	}
}
