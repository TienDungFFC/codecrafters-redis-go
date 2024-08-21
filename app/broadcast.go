package main

import (
	"fmt"
	"net"
)

func handleBroadcast(rawBuf []byte, commandMS int64) {
	if commandMS > _metaInfo.lastCommandMS.Load() {
		_metaInfo.lastCommandMS.Store(commandMS)
		_metaInfo.processedSlaves.Store(0)
	}
	for _, slave := range _metaInfo.slaves {
		go func(_slave net.Conn) {
			// send raw buf
			_, err := _slave.Write(rawBuf)
			if err != nil {
				fmt.Printf("failed to send broadcast %s", string(rawBuf))
				return
			}
			if _metaInfo.lastCommandMS.Load() == commandMS {
				_metaInfo.processedSlaves.Add(1)
			}
		}(slave)
	}
}
