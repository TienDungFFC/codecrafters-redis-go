package main

import (
	"fmt"
	"strconv"
)

type Command struct {
	Args [][]byte
}

func readCommand(cmd []byte) ([][]byte, error) {
	if cmd[0] == '*' {
		lArr, _ := strconv.Atoi(string(cmd[1]))
		args := make([][]byte, lArr)
		i := 2
		iArr := 0
		for ; i < len(cmd); i++ {
			if cmd[i] == '\r' || cmd[i] == '\n' {
				continue
			}
			if cmd[i] == '$' {
				fmt.Println("newBulkString: ", 1)

				lBulk, _ := strconv.Atoi(string(cmd[i+1]))
				newBulkStr := []byte{}
				for j := i + 2; j <= i+lBulk+3; j++ {
					if cmd[j] == '\r' || cmd[j] == '\n' {
						continue
					} else {
						newBulkStr = append(newBulkStr, cmd[j])
					}
				}
				fmt.Println("newBulkString: ", string(newBulkStr))
				args[iArr] = newBulkStr
				iArr++
			}
		}
		fmt.Println("args: ", args)
		return args, nil
	}
	return [][]byte{}, nil
}
