package main

import (
	"fmt"
	"strconv"
	"strings"
)

func parseString(input string) ([]string, error) {
	// Check if input starts with '*'
	if !strings.HasPrefix(input, "*") {
		return nil, fmt.Errorf("input does not start with '*'")
	}

	// Trim the initial '*'
	input = input[1:]

	// Find the number of elements
	index := strings.Index(input, "\r\n")
	if index == -1 {
		return nil, fmt.Errorf("invalid input format: missing '\\r\\n'")
	}

	numElementsStr := input[:index]
	numElements, err := strconv.Atoi(numElementsStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse number of elements: %v", err)
	}

	// Prepare to parse each element
	var elements []string
	input = input[index+2:] // Move past "\r\n"

	for i := 0; i < numElements; i++ {
		// Find the length of the element string
		if !strings.HasPrefix(input, "$") {
			return nil, fmt.Errorf("invalid input format: missing '$'")
		}

		input = input[1:] // Trim the initial '$'

		// Find the length of the current element
		index = strings.Index(input, "\r\n")
		if index == -1 {
			return nil, fmt.Errorf("invalid input format: missing '\\r\\n'")
		}

		lengthStr := input[:index]
		length, err := strconv.Atoi(lengthStr)
		if err != nil {
			return nil, fmt.Errorf("failed to parse element length: %v", err)
		}

		// Extract the element itself
		startIndex := index + 2
		endIndex := startIndex + length
		if endIndex > len(input) {
			return nil, fmt.Errorf("input format exceeds expected length")
		}

		element := input[startIndex:endIndex]
		elements = append(elements, element)

		// Move past the current element in the input string
		input = input[endIndex+2:]
	}

	return elements, nil
}

func ptr[T any](t T) *T {
	return &t
}
