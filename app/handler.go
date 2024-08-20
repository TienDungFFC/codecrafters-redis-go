package main

import (
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"
)

const (
	ARRAY       byte = '*'
	BULK_STRING byte = '$'
)

const (
	ECHO     = "echo"
	PING     = "ping"
	SET      = "set"
	GET      = "get"
	INFO     = "info"
	REPLCONF = "replconf"
	PSYNC    = "psync"
)

var infoRepl = []string{"role", "connected_slaves", "master_replid", "master_repl_offset", "second_repl_offset", "repl_backlog_active", "repl_backlog_size",
	"repl_backlog_first_byte_offset", "repl_backlog_histlen"}

type Value struct {
	val []byte
	px  time.Time
}

var mSet = make(map[string]Value)

func (s *Server) handler(str []byte) {
	args, _ := readCommand(str)
	s.cmd.Args = args
	s.cmd.Raw = str
	fmt.Println("args: ", string(str))
	s.handlecommand(args)
}

func (s *Server) handlecommand(args [][]byte) {
	cmd := strings.ToLower(string(args[0]))
	switch cmd {
	case ECHO:
		s.handleEcho()
	case PING:
		s.writeData(simpleStringResponse("PONG"))
	case SET:
		v := Value{
			val: args[2],
			px:  time.Time{},
		}
		if len(args) > 3 {
			t, _ := strconv.Atoi(string(args[4]))
			n := time.Now()
			ex := n.Add(time.Duration(t) * time.Millisecond)
			v.px = ex
		}
		mSet[string(args[1])] = v
		fmt.Println("map after set: ", mSet)
		if s.role == MASTER {
			s.writeData(simpleStringResponse("OK"))
		}
		if s.role == MASTER && len(slaves) > 0 {
			for _, slave := range slaves {
				(*slave).Write(s.cmd.Raw)
			}
		}
	case GET:
		val, ok := mSet[string(args[1])]
		fmt.Println("map: ", mSet)
		if ok && (val.px.IsZero() || time.Now().Before(val.px)) {
			s.writeData(bulkStringResponse(strings.TrimSpace(string(val.val))))
		} else if time.Now().After(val.px) {
			s.writeData(nullBulkStringResponse())
		} else {
			s.writeData(nullBulkStringResponse())
		}
	case INFO:
		if string(args[1]) == "replication" {
			s.writeData(s.infoReplicationResponse())
		}
	case REPLCONF:
		s.writeData(simpleStringResponse("OK"))
		fmt.Println("append scon: ", s.conn)

	case PSYNC:
		s.writeData(s.fullResync())
		emptyRDBStr := "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"
		emptyRDBByte, err := hex.DecodeString(emptyRDBStr)
		if err != nil {
			fmt.Println("Error decoding", err)
		}
		s.writeData(EncodeFile(emptyRDBByte))
		slaves = append(slaves, &s.conn)

	default:
		s.writeData(simpleStringResponse("unknown"))
	}
}

func (s *Server) handleEcho() {
	s.writeData(simpleStringResponse(string(s.cmd.Args[1])))
}

func (s *Server) writeData(str string) {
	_, err := s.conn.Write([]byte(str))
	if err != nil {
		fmt.Println("Error writing connection: ", err.Error())
	}
}

func simpleStringResponse(s string) string {
	return fmt.Sprintf("+%v\r\n", s)
}

func nullBulkStringResponse() string {
	return "$-1\r\n"
}
func bulkStringResponse(s string) string {
	return fmt.Sprintf("$%d\r\n%v\r\n", len(s), s)
}

func (s *Server) infoReplicationResponse() string {
	infoResp := s.getRoleInfo() + s.getReplOffset() + s.getReplId()
	return fmt.Sprintf("$%d%v\r\n", len(infoResp)-2, infoResp)
}

func (s *Server) getRoleInfo() string {
	return fmt.Sprintf("\r\nrole:%v", s.role)
}

func (s *Server) getReplId() string {
	return fmt.Sprintf("\r\nmaster_replid:%v", s.repliId)
}

func (s *Server) getReplOffset() string {
	return fmt.Sprintf("\r\nmaster_repl_offset:%v", s.replOffset)
}

func (s *Server) fullResync() string {
	return fmt.Sprintf("+FULLRESYNC %s %s\r\n", s.repliId, s.replOffset)
}

func EncodeFile(buf []byte) string {
	return fmt.Sprintf("$%d\r\n%s", len(buf), string(buf))
}
