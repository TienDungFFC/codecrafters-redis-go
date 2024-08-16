package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
)

const (
	MASTER Role = "master"
	SLAVE  Role = "slave"
)

type Role string

var (
	port      *string
	replicaof *string
)

type Server struct {
	role       Role
	port       string
	repliId    string
	replOffset string
	replicaof *string
}

func init() {
	port = flag.String("port", "6379", "Port to connect to")
	replicaof = flag.String("replicaof", "", "Replica of master")
	flag.Parse()
}

func NewServer() Server {
	role := MASTER
	if *replicaof != "" {
		role = SLAVE
	}
	return Server{
		role:       role,
		port:       *port,
		repliId:    "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
		replOffset: "0",
		replicaof: replicaof,
	}
}

func main() {
	fmt.Println("Logs from your program will appear here!")

	server := NewServer()
	if server.role == SLAVE {
		rep := strings.Split(*server.replicaof, " ")
		conn, err := server.connectMaster(rep[0], rep[1])
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		defer conn.Close()
		_, err = conn.Write([]byte("*1\r\n$4\r\nPING\r\n"))
		fmt.Println("connecction: ", conn)
		if err != nil {
			fmt.Println("Sending PING error")
		}
	}
	l, err := server.ListenNetwork()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}

		go server.handleConnection(conn)
	}
}

func (s Server) connectMaster(host, port string) (net.Conn, error) {
	conn, err := net.Dial("tcp", fmt.Sprintf("%v:%v", host, port))
	if err != nil {
        fmt.Println("Error:", err)
        return nil, err
    }
	return conn, nil
}

func (s Server) ListenNetwork() (net.Listener, error) {
	l, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%v", s.port))
	if err != nil {
		return nil, err
	}
	return l, nil
}

func (s Server) handleConnection(conn net.Conn) {
	for {
		buf := make([]byte, 1024)
		n, err := conn.Read(buf)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			fmt.Println("Error reading data: ", err.Error())
			continue
		}

		res := s.handler(buf[:n])
		// res := string(buf[:n])
		_, err = conn.Write([]byte(res))
		if err != nil {
			fmt.Println("Error writing connection: ", err.Error())
			os.Exit(1)
		}
	}
}
