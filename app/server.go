package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
)

func main() {
	fmt.Println("Logs from your program will appear here!")
	var port = flag.String("port", "6379", "Port to listen on")
	flag.Parse()
	fmt.Println("test ports: ", *port)
	l, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%v", *port))
	// l, err := net.Listen("tcp", "0.0.0.0:6379")

	if err != nil {
		fmt.Printf("Failed to bind to port %s\n", *port)
		os.Exit(1)
	}
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}

		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	for {
		buf := make([]byte, 1024)
		n, err := conn.Read(buf)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			fmt.Println("Error reading data: ", err.Error())
			continue;
		}

		res := handler(buf[:n])
		// res := string(buf[:n])
		_, err = conn.Write([]byte(res))
		if err != nil {
			fmt.Println("Error writing connection: ", err.Error())
			os.Exit(1)
		}
	}
}
