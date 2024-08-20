package main

import (
	"strconv"
)

type Command struct {
	Raw  []byte
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
				lBulk, cByte := parseInt(cmd[i+1:])
				newBulkStr := []byte{}
				for j := i + cByte + 1; j <= i+lBulk+cByte+2; j++ {
					if cmd[j] == '\r' || cmd[j] == '\n' {
						continue
					} else {
						newBulkStr = append(newBulkStr, cmd[j])
					}
				}
				args[iArr] = newBulkStr
				iArr++
			}
		}
		return args, nil
	} else if cmd[0] == '+' {
		return nil, nil
	}
	return [][]byte{}, nil
}

func parseInt(arg []byte) (int, int) {
	res := []byte{}
	cByte := 0
	for i := 0; i < len(arg); i++ {
		if arg[i] != '\r' {
			res = append(res, arg[i])
			cByte++
		} else {
			break
		}
	}
	nByte, _ := strconv.Atoi(string(res))
	return nByte, cByte
}
