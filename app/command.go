package main

import (
	"fmt"
	"net"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	TYPE_STRING valueType = "string"
	TYPE_STREAM valueType = "stream"
)

type valueType string
type store struct {
	typ      valueType
	value    string
	expireAt time.Time
}

type Command struct {
	Raw  string
	Args []string
}

var lock sync.Mutex
var _map sync.Map
var (
	ackReceived = make(chan bool)
)

func (h *Handler) handleCommand(rawStr string) string {
	conn := h.conn
	rawBuf := []byte(rawStr)
	strs, err := parseString(rawStr)
	if err != nil {
		fmt.Printf("failed to read data %+v\n", err)
		return ""
	}
	if h.isExecute {
		fmt.Printf("is executing in localhost:%d got %q\n", _metaInfo.port, strs)

	} else {

		fmt.Printf("localhost:%d got %q\n", _metaInfo.port, strs)
	}

	command := strings.ToLower(strs[0])
	byteLen := len(rawBuf)

	var reply string
	var shouldUpdateByte bool

	transExceptCmd := []string{"exec", "discard"}
	if h.startTransaction && !slices.Contains(transExceptCmd, command) && !h.isExecute {
		h.queueTrans = append(h.queueTrans, Command{Raw: rawStr, Args: strs})
		h.Write(h.QueuedResponse())
		fmt.Println("command when start transaction: ", command)
		return ""
	}
	switch command {
	case "ping":
		if _metaInfo.isMaster() {
			reply = "PONG"
			conn.Write([]byte(fmt.Sprintf("+%s\r\n", reply)))
		}
		shouldUpdateByte = true
	case "echo":
		reply = strs[1]
		conn.Write([]byte(fmt.Sprintf("+%s\r\n", reply)))
	case "set":
		handleSet(strs[1:])
		now := time.Now()
		if _metaInfo.isMaster() {
			handleBroadcast(rawBuf, now.UnixMilli())
			reply = "OK"
			if h.isExecute {
				return fmt.Sprintf("+%s\r\n", reply)
			}
			conn.Write([]byte(fmt.Sprintf("+%s\r\n", reply)))

		}
		shouldUpdateByte = true
		_metaInfo.startSet.Store(true)
	case "get":
		resp, ok := handleGet(strs[1])
		if ok {
			reply = resp
			if h.isExecute {
				return fmt.Sprintf("$%d\r\n%s\r\n", len(reply), reply)
			}
			conn.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(reply), reply)))
		} else {
			reply = "-1"
			conn.Write([]byte(fmt.Sprintf("$%s\r\n", reply)))
		}
	case "info":
		replies := handleInfo()
		sendBulkString(conn, replies)
	case "replconf":
		if len(strs) == 3 && strs[1] == "GETACK" && strs[2] == "*" {
			length := fmt.Sprintf("%d", _metaInfo.processedBytes.Load())
			conn.Write([]byte(fmt.Sprintf("*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$%d\r\n%s\r\n", len(length), length)))
		} else if len(strs) == 3 && strs[1] == "ACK" {
			fmt.Printf("thx for ack %s \n", conn.RemoteAddr().String())
			ackReceived <- true
		} else {
			reply = "OK"
			conn.Write([]byte(fmt.Sprintf("+%s\r\n", reply)))
		}
		shouldUpdateByte = true
	case "psync":
		conn.Write([]byte(fmt.Sprintf("+FULLRESYNC %s %d\r\n", _metaInfo.masterReplID, *_metaInfo.masterReplOffset)))
		time.Sleep(100 * time.Millisecond)
		fullByte := getEmptyRDBByte()
		conn.Write([]byte(fmt.Sprintf("$%d\r\n%s", len(fullByte), fullByte)))

		_metaInfo.addSlave(conn)
	case "wait":
		go handleWait(conn, strs[1], strs[2])
	case "config":
		if strs[2] == "dir" {
			conn.Write([]byte(fmt.Sprintf("*2\r\n$3\r\ndir\r\n$%d\r\n%s\r\n", len(_metaInfo.dir), _metaInfo.dir)))
		} else if strs[2] == "dbfilename" {
			conn.Write([]byte(fmt.Sprintf("*2\r\n$10\r\ndbfilename\r\n$%d\r\n%s\r\n", len(_metaInfo.dbFileName), _metaInfo.dbFileName)))
		}
	case "incr":
		v, ok := handleGet(strs[1])
		isNumeric := true
		iV, err := strconv.Atoi(v)
		if err != nil {
			isNumeric = false
		}
		if ok && isNumeric {
			iV++
			lock.Lock()
			handleSet([]string{strs[1], strconv.Itoa(iV)})
			lock.Unlock()

			if h.isExecute {
				return h.IntegerResponse(iV)
			}
			h.Write(h.IntegerResponse(iV))
		} else if ok && !isNumeric {
			if h.isExecute {
				return h.SimpleErrorResponse("ERR value is not an integer or out of range")
			}
			h.Write(h.SimpleErrorResponse("ERR value is not an integer or out of range"))
		} else {
			handleSet([]string{strs[1], "1"})
			if h.isExecute {
				return h.IntegerResponse(1)
			}
			h.Write(h.IntegerResponse(1))
		}
	case "multi":
		h.startTransaction = true
		h.Write(h.SimpleStringResponse("OK"))
	case "exec":
		if !h.startTransaction {
			h.Write(h.SimpleErrorResponse("ERR EXEC without MULTI"))
			return ""
		} else {
			if len(h.queueTrans) == 0 {
				h.Write(h.EmptyArrayResponse())
				h.startTransaction = false
				return ""
			}
			h.isExecute = true
			fmt.Println("is executing: ", h.queueTrans)
			cElement := 0
			res := []string{}
			for _, c := range h.queueTrans {
				cElement++
				res = append(res, h.handleCommand(c.Raw))
			}

			h.isExecute = false
			h.startTransaction = false
			h.Write(h.ArrayResponse(res))
		}
	case "discard":
		if !h.startTransaction {
			h.Write(h.SimpleErrorResponse("ERR DISCARD without MULTI"))
			return ""
		} else {
			h.queueTrans = []Command{}
			h.Write(h.SimpleStringResponse("OK"))
			h.startTransaction = false
		}
	case "keys":

		c := 0
		tmp := ""
		_map.Range(func(key, value interface{}) bool {
			tmp += fmt.Sprintf("$%d\r\n%s\r\n", len(key.(string)), key)
			c++
			return true
		})
		res := fmt.Sprintf("*%d\r\n%s", c, tmp)
		h.Write(res)

	case "type":
		v, ok := _map.Load(strs[1])
		if !ok {
			_, exist := stream[strs[1]]
			if exist {
				h.Write(h.SimpleStringResponse(string(TYPE_STREAM)))
			} else {
				h.Write(h.SimpleStringResponse("none"))
			}
		} else {
			h.Write(h.SimpleStringResponse(string(v.(store).typ)))
		}
	case "xadd":
		id := strs[2]
		s, ok := stream[strs[1]]
		key := strs[3]
		val := strs[4]
		ids := strings.Split(id, "-")

		_, err := s.ValidateEntryId(id)
		if err != nil {
			h.Write(h.SimpleErrorResponse(fmt.Sprint(err)))
			return ""
		}
		now := time.Now().Unix()
		eId := EntryId{
			timestamp: int(now),
			seq:       0,
		}
		if ok {

			sKV := []StreamEntryValue{}
			sKV = append(sKV, StreamEntryValue{
				Key:   key,
				Value: val,
			})

			if id != "*" {
				t, _ := ConverIdEntryInt(ids)
				eId, ok := s.FindEntryId(t)
				if ok {
					eId.seq++
				}
			}

			s.entries = append(s.entries, &StreamEntry{
				Id: eId,
				KV: sKV,
			})
			s.lastId = &eId
			h.Write(h.BulkStringResponse(s.EntryIdToString(eId)))
		} else {
			ss := NewStreamStore()
			sKV := []StreamEntryValue{}
			sKV = append(sKV, StreamEntryValue{
				Key:   key,
				Value: val,
			})

			if id != "*" && len(ids) > 1 {
				if ids[1] == "*" {
					t, _ := strconv.Atoi(ids[0])
					se := 0
					if t == 0 {
						se = 1
					}
					eId.timestamp = t
					eId.seq = se
				} else {
					t, se := ConverIdEntryInt(ids)
					eId.timestamp = t
					eId.seq = se
				}
			}

			sEntry := NewStreamEntry(eId, sKV)
			ss.entries = append(ss.entries, sEntry)
			ss.lastId = &eId
			stream[strs[1]] = ss
			h.Write(h.BulkStringResponse(s.EntryIdToString(eId)))
		}
	}

	if !_metaInfo.isMaster() && shouldUpdateByte {
		fmt.Println("byteLen: ", byteLen)
		_metaInfo.processedBytes.Add(int32(byteLen))
	}
	return "success"
}

func handleSet(strs []string) {
	now := time.Now()
	key := strs[0]
	value := strs[1]

	stored := store{
		value: value,
		typ:   TYPE_STRING,
	}

	if len(strs) > 2 {
		switch strings.ToLower(strs[2]) {
		case "px":
			ms, err := strconv.Atoi(strs[3])
			if err != nil {
				os.Exit(-1)
			}
			stored.expireAt = now.Add(time.Millisecond * time.Duration(ms))
		}
	}

	_map.Store(key, stored)
}

func handleGet(key string) (string, bool) {
	now := time.Now()
	value, ok := _map.Load(key)
	if !ok {
		return "", false
	}
	stored, ok := value.(store)
	if !ok {
		return "", false
	}
	if expireAt := stored.expireAt; !expireAt.IsZero() && expireAt.Before(now) {
		return "", false
	}

	return stored.value, true
}

func handleInfo() []string {
	var reply []string

	if _metaInfo.port == 6379 {
		reply = append(reply, "role:master")
	} else {
		reply = append(reply, "role:slave")
	}

	if len(_metaInfo.masterReplID) > 0 {
		reply = append(reply, fmt.Sprintf("master_replid:%s", _metaInfo.masterReplID))
	}
	if _metaInfo.masterReplOffset != nil {
		reply = append(reply, fmt.Sprintf("master_repl_offset:%d", *_metaInfo.masterReplOffset))
	}

	return reply
}

func handleWait(conn net.Conn, replicaStr, waitMSStr string) {
	for _, slave := range _metaInfo.slaves {
		go func(_slave net.Conn) {
			_slave.Write([]byte("*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n"))
		}(slave)
	}

	replica, _ := strconv.Atoi(replicaStr)
	waitMS, _ := strconv.Atoi(waitMSStr)

	timer := time.After(time.Duration(waitMS) * time.Millisecond)
	ackNum := 0
	if !_metaInfo.startSet.Load() {
		conn.Write([]byte(fmt.Sprintf(":%d\r\n", len(_metaInfo.slaves))))
		return
	}
	for ackNum < replica {
		select {
		case <-ackReceived:
			fmt.Printf("received ack\n")
			ackNum++
		case <-timer:
			fmt.Printf("timeout reached %d\n", waitMS)
			conn.Write([]byte(fmt.Sprintf(":%d\r\n", ackNum)))
			return
		}
	}
	conn.Write([]byte(fmt.Sprintf(":%d\r\n", ackNum)))
	return
}

func (h *Handler) Write(s string) {
	h.conn.Write([]byte(s))
}
func (h *Handler) IntegerResponse(i int) string {
	return fmt.Sprintf(":%d\r\n", i)
}

func (h *Handler) SimpleErrorResponse(err string) string {
	return fmt.Sprintf("-%s\r\n", err)
}

func (h *Handler) SimpleStringResponse(s string) string {
	return fmt.Sprintf("+%s\r\n", s)
}

func (h *Handler) BulkStringResponse(s string) string {
	return fmt.Sprintf("$%d\r\n%s\r\n", len(s), s)
}

func (h *Handler) EmptyArrayResponse() string {
	return "*0\r\n"
}

func (h *Handler) QueuedResponse() string {
	return "+QUEUED\r\n"
}

func (h *Handler) ArrayResponse(responses []string) string {
	resArr := fmt.Sprintf("*%d\r\n", len(responses))
	for _, s := range responses {
		resArr += s
	}
	return resArr
}
