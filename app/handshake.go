package main

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"time"
)

func handshake() {
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", _metaInfo.masterHost, _metaInfo.masterPort))
	if err != nil {
		fmt.Printf("failed to dial master")
		os.Exit(-1)
	}

	// send PING
	_, err = conn.Write([]byte("*1\r\n$4\r\nPING\r\n"))
	if err != nil {
		fmt.Printf("failed to send ping")
		os.Exit(-1)
	}
	time.Sleep(time.Millisecond * 100)

	// send REPLCONF
	_, err = conn.Write([]byte(fmt.Sprintf("*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$%d\r\n%d\r\n", len(strconv.Itoa(_metaInfo.masterPort)), _metaInfo.port)))
	if err != nil {
		fmt.Printf("failed to send first REPLCONF")
		os.Exit(-1)
	}
	time.Sleep(time.Millisecond * 100)

	_, err = conn.Write([]byte("*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n"))
	if err != nil {
		fmt.Printf("failed to send second REPLCONF")
		os.Exit(-1)
	}
	time.Sleep(time.Millisecond * 100)

	// *3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n
	_, err = conn.Write([]byte("*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n"))
	if err != nil {
		fmt.Printf("failed to send second REPLCONF")
		os.Exit(-1)
	}
	go handleClient(conn)
}
