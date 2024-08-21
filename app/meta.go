package main

import (
	"encoding/hex"
	"flag"
	"net"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
)

const (
	_emptyRDBHex = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"
)

type metaInfo struct {
	port             int
	masterHost       string
	masterPort       int
	masterReplID     string
	masterReplOffset *int
	slaves           []net.Conn
	processedBytes   atomic.Int32

	// command related
	lastCommandMS   atomic.Int64
	processedSlaves atomic.Int32
	startSet        atomic.Bool

	// config
	dir        string
	dbFileName string
}

func (mi *metaInfo) isMaster() bool {
	return len(mi.masterHost) == 0
}

func (mi *metaInfo) addSlave(conn net.Conn) {
	mi.slaves = append(mi.slaves, conn)
}

var (
	_metaInfo metaInfo
)

func initMeta() {
	// Define the port flag
	flag.IntVar(&_metaInfo.port, "port", 6379, "Port number to listen on")

	replicaOf := ""
	flag.StringVar(&replicaOf, "replicaof", "", "Replication info")
	flag.StringVar(&_metaInfo.dir, "dir", "", "Dir")
	flag.StringVar(&_metaInfo.dbFileName, "dbfilename", "", "DB file name")

	flag.Parse()

	if len(replicaOf) > 0 {
		replicaInfo := strings.Split(replicaOf, " ")
		_metaInfo.masterHost = replicaInfo[0]
		masterPort, err := strconv.Atoi(replicaInfo[1])
		if err != nil {
			os.Exit(-1)
		}
		_metaInfo.masterPort = masterPort
	} else {
		_metaInfo.masterReplID = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
		_metaInfo.masterReplOffset = ptr(0)
	}

}

func getEmptyRDBByte() []byte {
	// Decode the hexadecimal string
	byteSlice, err := hex.DecodeString(_emptyRDBHex)
	if err != nil {
		os.Exit(-1)
	}
	return byteSlice
}
