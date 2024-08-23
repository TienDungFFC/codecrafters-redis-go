package main

import (
	"fmt"
	"net"
	"os"
)

type Handler struct {
	conn             net.Conn
	startTransaction bool
	queueTrans       []Command
	isExecute        bool
}

func NewHandler(c net.Conn) *Handler {
	return &Handler{
		conn:             c,
		startTransaction: false,
		queueTrans:       make([]Command, 0),
		isExecute:        false,
	}
}
func main() {
	initMeta()

	r := RDB{}
	r.LoadFile()
	if r.file != nil {
		b, err := os.ReadFile(_metaInfo.dir + "/" + _metaInfo.dbFileName) // just pass the file name
		if err != nil {
			fmt.Print(err)
		}

		fmt.Println(b) // print the content as 'bytes'

		str := string(b) // convert content to a 'string'

		fmt.Println(str) // print the content as a 'string'

		r.ReadDB()
		r.file.Close()
	} else {
		fmt.Println("File doesn't exists")
	}

	port := _metaInfo.port
	// Listen for incoming connections
	listener, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", port))
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	// Ensure we teardown the server when the program exits
	defer listener.Close()

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
