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
)

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
	fmt.Println("part[0]: ", parts[1])
	fmt.Println("newStr: ", newStr)
	
	switch cmd {
	case ECHO:
		return stringResponse(strings.TrimSpace(parts[3]))
	case PING: 
		return stringResponse("PONG")
	}
	return stringResponse("unknown")
}

func stringResponse(s string) string {
	return fmt.Sprintf("+%v\r\n", s)
}
// func main() {
// 	fmt.Println(handler("*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n"))
// }