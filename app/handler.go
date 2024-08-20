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
	WAIT     = "wait"
)

type Value struct {
	val []byte
	px  time.Time
}

var mSet = make(map[string]Value)

func (s *Server) handler(str []byte) {
	args, _ := readCommand(str)
	s.cmd.Args = args
	s.cmd.Raw = str
	if len(args) == 0 {
		return
	}
	s.handlecommand(args)
}

func (s *Server) handlecommand(args [][]byte) {
	cmd := strings.ToLower(string(args[0]))
	fmt.Println("handlecommand: ", string(args[0]))
	switch cmd {
	case ECHO:
		s.handleEcho()
	case PING:
		if s.role == MASTER {
			s.writeData(simpleStringResponse("PONG"))
		}
		s.offset += len(s.cmd.Raw)
	case SET:
		s.offset += len(s.cmd.Raw)
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
		if strings.ToLower(string(args[1])) == "getack" {
			fmt.Println("get ack:", string(args[1]))
			s.writeData(s.replConfResponse())
			s.offset += len(s.cmd.Raw)
		} else if strings.ToLower(string(args[1])) == "ack" {
			s.ackChan <- true
		} else {
			s.writeData(simpleStringResponse("OK"))
		}

	case PSYNC:
		s.writeData(s.fullResync())
		emptyRDBStr := "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"
		emptyRDBByte, err := hex.DecodeString(emptyRDBStr)
		if err != nil {
			fmt.Println("Error decoding", err)
		}
		s.writeData(EncodeFile(emptyRDBByte))
		slaves = append(slaves, &s.conn)
		time.Sleep(1 * time.Second)
		for _, slave := range slaves {
			(*slave).Write([]byte("*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n"))
		}
	case WAIT:
		nOfRepl, _ := strconv.Atoi(string(args[1]))
		duration, _ := strconv.Atoi(string(args[2]))

		if len(mSet) == 0 {
			s.writeData(integersResponse(len(slaves)))
			return
		}

		for _, slave := range slaves {
			go func() {
				(*slave).Write([]byte("*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n"))
			}()
			time.Sleep(400 * time.Millisecond)
		}

		timer := time.After(time.Duration(duration) * time.Millisecond)
		ackCount := 0

		for ackCount < nOfRepl {
			fmt.Println("ackCount: ", duration)
			fmt.Println("timer: ", timer)

			select {
			case <-s.ackChan:
				fmt.Println("increasing ackcount: ", ackCount)
				ackCount++
			case <-timer:
				s.writeData(integersResponse(1))
				return
			}
		}

		s.writeData(integersResponse(1))
	default:
		s.writeData(simpleStringResponse("unknown"))
	}
}

func (s *Server) handleEcho() {
	s.writeData(simpleStringResponse(string(s.cmd.Args[1])))
}

func (s *Server) replConfResponse() string {
	sOffset := strconv.Itoa(s.offset)
	lOfs := len([]byte(sOffset))
	return fmt.Sprintf("*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$%d\r\n%d\r\n", lOfs, s.offset)
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

func integersResponse(i int) string {
	return fmt.Sprintf(":%d\r\n", i)
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
