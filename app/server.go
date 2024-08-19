package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"time"
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
	replicaof  *string
	cmd        Command
	conn       net.Conn
	cRepl      []*net.Conn
}

func init() {
	port = flag.String("port", "6379", "Port to connect to")
	replicaof = flag.String("replicaof", "", "Replica of master")
	flag.Parse()
}

func NewServer(conn net.Conn, r Role) *Server {

	return &Server{
		role:       r,
		port:       *port,
		repliId:    "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
		replOffset: "0",
		replicaof:  replicaof,
		conn:       conn,
		cRepl:      make([]*net.Conn, 0),
	}
}

func main() {
	fmt.Println("Logs from your program will appear here!")
	role := MASTER
	if *replicaof != "" {
		role = SLAVE
	}
	if role == SLAVE {
		rep := strings.Split(*replicaof, " ")
		conn, err := connectMaster(rep[0], rep[1])
		server := NewServer(conn, role)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		defer server.conn.Close()
		_, err = server.conn.Write([]byte("*1\r\n$4\r\nPING\r\n"))
		if err != nil {
			fmt.Println("Sending PING error")
		}
		time.Sleep(1 * time.Second)

		_, err = server.conn.Write([]byte("*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n6380\r\n"))
		if err != nil {
			fmt.Println("Sending PING error")
		}
		time.Sleep(1 * time.Second)

		_, err = server.conn.Write([]byte("*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n"))
		if err != nil {
			fmt.Println("Sending PING error")
		}
		time.Sleep(1 * time.Second)
		_, err = server.conn.Write([]byte("*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n"))
		if err != nil {
			fmt.Println("Sending PING error")
		}
	}
	l, err := ListenNetwork()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	for {
		conn, err := l.Accept()
		mServer := NewServer(conn, role)
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}

		go mServer.handleConnection(conn)
	}
}

func connectMaster(host, port string) (net.Conn, error) {
	conn, err := net.Dial("tcp", fmt.Sprintf("%v:%v", host, port))
	if err != nil {
		fmt.Println("Error:", err)
		return nil, err
	}
	return conn, nil
}

func ListenNetwork() (net.Listener, error) {
	l, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%v", *port))
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
		s.handler(buf[:n])
		// res := string(buf[:n])
		// _, err = conn.Write([]byte(res))
		// if err != nil {
		// 	fmt.Println("Error writing connection: ", err.Error())
		// 	os.Exit(1)
		// }
	}
}
