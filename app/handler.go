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

type Value struct {
	val []byte
	px  time.Time
}

var mSet = make(map[string]Value)

func (s Server) handler(str []byte) string {
	// fChar := str[0]
	// switch fChar {
	// case ARRAY:
	// 	return handler(str[4:])
	// case BULK_STRING:
	// 	return handleString(str)
	// default:
	// 	return "PONG"
	// }
	fmt.Println("string: ", str)
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
			return infoReplicationResponse(string(s.role))
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
	fmt.Println("len(s): ", len(s))
	fmt.Println("s: ", s)
	return fmt.Sprintf("$%d\r\n%v\r\n", len(s), s)
}

func infoReplicationResponse(s string) string {
	return fmt.Sprintf("$%d\r\nrole:%v\r\n", len(s)+5, s)
}
