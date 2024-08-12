package main

import (
	"fmt"
	"strconv"
	"strings"
)

const (
	ARRAY byte = '*'
	BULK_STRING byte = '$'
)

const (
	ECHO = "echo"
	PING = "ping"
	SET = "set"
	GET = "get"
)

var mSet = make(map[string]string)

func handler(str []byte) string {
	fChar := str[0]
	switch fChar {
	case ARRAY:
		return handler(str[4:])
	case BULK_STRING: 
		return handleString(str)
	default:
		return "PONG"
	}
	
}

func handleCommand(command string, orgStr []byte) string {
	lCmd := strings.ToLower(command)
	var output string
	switch lCmd {
	case ECHO:
		lCmd, err := strconv.Atoi(string(orgStr[1]))
		if err != nil {
			fmt.Println("Error converting str to int", err)
		}
		newStr := orgStr[6+lCmd: len(orgStr) - 1]
		output = string(newStr)
	}
	return output
}

func handleString(str []byte) string {
	newStr := string(str)
	parts := strings.Split(newStr, "\n")
	cmd := strings.ToLower(strings.TrimSpace(parts[1]))

	switch cmd {
	case ECHO:
		return stringResponse(strings.TrimSpace(parts[3]))
	case PING: 
		return stringResponse("PONG")
	case SET:
		mSet[strings.TrimSpace(parts[3])] = strings.TrimSpace(parts[5])
		return stringResponse("OK")
	case GET:
		val, ok := mSet[strings.TrimSpace(parts[3])]
		if ok {
			return bulkStringResponse(strings.TrimSpace(val))
		} else {
			return "$-1\r\n"
		}
		
	}
	return stringResponse("unknown")
}

func stringResponse(s string) string {
	return fmt.Sprintf("+%v\r\n", s)
}

func bulkStringResponse(s string) string {
	fmt.Println("len(s): ", len(s))
	fmt.Println("s: ", s)
	return fmt.Sprintf("$%d\r\n%v\r\n", len(s), s)
}
// func main() {
// 	fmt.Println(handler("*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n"))
// }