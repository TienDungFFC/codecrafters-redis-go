package main

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"
)

var (
	startDBSection byte = 254
)

type RDB struct {
	reader *bufio.Reader
	file   *os.File
}

func (r *RDB) LoadFile() {
	filename := _metaInfo.dir + "/" + _metaInfo.dbFileName
	file, err := os.Open(filename)
	if err != nil {
		// return nil, err
		fmt.Println("file doesn't exists")
	}
	reader := bufio.NewReader(file)
	r.reader = reader
	r.file = file
}

func (r *RDB) ReadDB() {
	for {
		t, err := r.reader.ReadByte()
		fmt.Println("t: ", string(t))
		if err != nil {
			return
		}
		if t != startDBSection {
			continue
		} else {
			dbNumber, err := r.readSize()
			if err != nil {
				panic(err)
			}
			fmt.Printf("Database number: %d\n", dbNumber)
			t, err = r.reader.ReadByte()
			if err != nil {
				panic(err)
			}
			fmt.Println("byte indices hash table size: ", t)
			if t != 0xFB {
				panic("Invalid database section")
			}
			hashTableSize, err := r.readSize()
			fmt.Println("size of hashtable: ", t)
			if err != nil {
				panic(err)
			}
			fmt.Printf("Hash table size: %d\n", hashTableSize)
			expiresSize, err := r.readSize()
			if err != nil {
				panic(err)
			}
			fmt.Printf("Expires size: %d\n", expiresSize)
			for i := 0; i < hashTableSize; i++ {
				valueType, err := r.reader.ReadByte()
				fmt.Println("value type: ", valueType)
				if err != nil {
					fmt.Println("error reading value type: ", err)
					panic(err)
				}
				// 0x00 value type is a string
				key, err := r.readString()
				if err != nil {
					fmt.Println("error reading string value: ", err)
					panic(err)
				}
				value, err := r.readString()
				if err != nil {
					fmt.Println("error reading value: ", err)
					panic(err)
				}
				redisValue := store{value: value}
				if expiresSize > 0 {
					expiryType, err := r.reader.ReadByte()
					if err != nil {
						panic(err)
					}
					// expiry time is in milliseconds
					switch expiryType {
					case 0xFC:
						var expiryMs int64
						err := binary.Read(r.reader, binary.LittleEndian, &expiryMs)
						if err != nil {
							panic(err)
						}
						redisValue.expireAt = time.UnixMilli(expiryMs)
					case 0xFD:
						var expirySec int64
						err := binary.Read(r.reader, binary.LittleEndian, &expirySec)
						if err != nil {
							panic(err)
						}
						redisValue.expireAt = time.Unix(expirySec*1000, 0)
					default:
						r.reader.UnreadByte()
					}
				}

				_map.Store(key, redisValue)
			}
		}
	}
}

func (r *RDB) readSize() (int, error) {
	t, err := r.reader.ReadByte()
	if err != nil {
		return 0, err
	}
	sizeType := t & 0xC0
	switch sizeType {
	// 0b00xxxxxx
	// 6-bit integer
	case 0x00:
		return int(t & 0x3F), nil
	// 0b01xxxxxx
	// 14-bit integer
	case 0x40:
		b, err := r.reader.ReadByte()
		if err != nil {
			return 0, err
		}
		// remove the first two bits and shift left 8 bits
		// then add the next byte by bitwise OR
		// this means we're getting a 14-bit integer
		return int(t&0x3F)<<8 | int(b), nil
	// 0b10xxxxxx
	// 32-bit integer
	case 0x80:
		// read 4 bytes into a 32-bit integer
		var size int32
		binary.Read(r.reader, binary.BigEndian, &size)
		return int(size), nil
	// 0b11000000
	// special encoding for strings
	case 0xC0:
		r.reader.UnreadByte()
		return -1, nil
	}
	return 0, errors.New("invalid size")
}

func (r *RDB) readString() (string, error) {
	size, err := r.readSize()
	if err != nil {
		return "", err
	}
	if size == -1 {
		return r.readSpecialEncodedString(r.reader)
	}
	str := make([]byte, size)
	_, err = r.reader.Read(str)
	if err != nil {
		return "", err
	}
	return string(str), nil
}

func (r *RDB) readSpecialEncodedString(reader *bufio.Reader) (string, error) {
	format, err := reader.ReadByte()
	if err != nil {
		return "", err
	}
	switch format {
	// 8 bit integer string
	case 0xC:
		var b int8
		err := binary.Read(reader, binary.LittleEndian, &b)
		// read 8-bit integer
		if err != nil {
			return "", err
		}
		return strconv.Itoa(int(b)), nil
	// 16 bit integer string
	case 0xC1:
		var b int16
		err := binary.Read(reader, binary.LittleEndian, &b)
		if err != nil {
			return "", err
		}
		return strconv.Itoa(int(b)), nil
	// 32 bit integer string
	case 0xC2:
		var b int32
		err := binary.Read(reader, binary.LittleEndian, &b)
		if err != nil {
			return "", err
		}
		return strconv.Itoa(int(b)), nil
	// LZF compressed string
	case 0xC3:
		panic("LZF compressed strings not supported")
	}
	return "", errors.New("invalid special encoding")
}
