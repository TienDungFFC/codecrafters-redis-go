package main

import (
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
	ECHO = "echo"
	PING = "ping"
	SET  = "set"
	GET  = "get"
	INFO = "info"
)

var infoRepl = []string{"role", "connected_slaves", "master_replid", "master_repl_offset", "second_repl_offset", "repl_backlog_active", "repl_backlog_size",
	"repl_backlog_first_byte_offset", "repl_backlog_histlen"}

type Value struct {
	val []byte
	px  time.Time
}

var mSet = make(map[string]Value)

func (s Server) handler(str []byte) string {
	args, _ := readCommand(str)
	return s.handlecommand(args)
}

func (s Server) handlecommand(args [][]byte) string {
	cmd := strings.ToLower(string(args[0]))

	switch cmd {
	case ECHO:
		return stringResponse(string(args[1]))
	case PING:
		return stringResponse("PONG")
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
		return stringResponse("OK")
	case GET:
		val, ok := mSet[string(args[1])]

		if ok && (val.px.IsZero() || time.Now().Before(val.px)) {
			return bulkStringResponse(strings.TrimSpace(string(val.val)))
		} else if time.Now().After(val.px) {
			return nullBulkStringResponse()
		} else {
			return nullBulkStringResponse()
		}
	case INFO:
		if string(args[1]) == "replication" {
			return s.infoReplicationResponse()
		}
	}
	return stringResponse("unknown")
}

func stringResponse(s string) string {
	return fmt.Sprintf("+%v\r\n", s)
}

func nullBulkStringResponse() string {
	return "$-1\r\n"
}
func bulkStringResponse(s string) string {
	return fmt.Sprintf("$%d\r\n%v\r\n", len(s), s)
}

func (s Server) infoReplicationResponse() string {
	infoResp := s.getRoleInfo() + s.getReplId() + s.getReplOffset()
	return fmt.Sprintf("$%d%v", len(infoResp), infoResp)
}

func (s Server) getRoleInfo() string {
	return fmt.Sprintf("\r\nrole:%v", s.role)
}

func (s Server) getReplId() string {
	return fmt.Sprintf("\r\nmaster_replid:%v", s.repliId)
}

func (s Server) getReplOffset() string {
	return fmt.Sprintf("\r\nmaster_repl_offset:%v", s.replOffset)
}
