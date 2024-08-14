package main

import (
	"reflect"
	"testing"
)

func TestReadCommand(t *testing.T) {
	tests := []struct {
		name        string
		input       []byte
		expected    [][]byte
		expectError bool
	}{
		// {
		// 	name:        "Valid command with 2 arguments",
		// 	input:       []byte("*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n"),
		// 	expected:    [][]byte{[]byte("GET"), []byte("key")},
		// 	expectError: false,
		// },
		// *5\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n\$2\r\npx\r\n$3\r\n100\r\n
		{
			name:        "Valid command with 5 arguments",
			input:       []byte("*5\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n$2\r\npx\r\n$3\r\n100\r\n"),
			expected:    [][]byte{[]byte("SET"), []byte("foo"), []byte("bar"), []byte("px"), []byte("100")},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := readCommand(tt.input)

			if (err != nil) != tt.expectError {
				t.Fatalf("expected error: %v, got: %v", tt.expectError, err)
			}

			if !reflect.DeepEqual(result, tt.expected) {
				t.Errorf("expected: %v, got: %v", tt.expected, result)
			}
		})
	}
}
