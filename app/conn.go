package main

import (
	"fmt"
	"regexp"
	"strconv"
)

func (h *Handler) handleClient() {
	for {
		conn := h.conn
		// Read data
		buf := make([]byte, 1024)
		n, err := conn.Read(buf)
		if err != nil {
			fmt.Printf("failed to read data\n")
			return
		}

		rawStr := string(buf[:n])
		fmt.Printf("raw str %s\n", strconv.Quote(rawStr))

		// can be multiple command
		commands := splitCommand(rawStr)

		for _, command := range commands {
			fmt.Printf("parsed command %q\n", strconv.Quote(command))
			strs, err := parseString(command)
			cmd := Command{
				Raw: command,
				Args: strs,
			}

			if err != nil {
				fmt.Printf("failed to read data %+v\n", err)
				return
			}
			fmt.Printf("localhost:%d got %q\n", _metaInfo.port, strs)
			h.handleCommand(cmd)
		}
	}
}

// splitCommand splits the input string only if '*' is followed by a number
func splitCommand(rawStr string) []string {
	var result []string

	// Regular expression to match "*<number>" pattern
	re := regexp.MustCompile(`\*(\d+)`)

	// Find all matches of the pattern
	matches := re.FindAllStringIndex(rawStr, -1)

	if len(matches) == 0 {
		// No valid '*' followed by a number; return the original string
		return []string{rawStr}
	}

	// Split the rawStr into parts based on the positions of the valid '*' patterns
	start := 0
	for _, match := range matches {
		// Extract the part between the last match and the current match
		result = append(result, rawStr[start:match[0]])
		start = match[0]
	}

	// Append the last part after the last match
	if start < len(rawStr) {
		result = append(result, rawStr[start:])
	}

	var finalResult []string
	for _, r := range result {
		if len(r) != 0 {
			finalResult = append(finalResult, r)
		}
	}

	return finalResult
}
