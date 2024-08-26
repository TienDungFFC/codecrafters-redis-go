package main

import (
	"errors"
)

var stream = make(map[string]*StreamStore)

type StreamEntryValue struct {
	Key   string
	Value string
}

type StreamEntry struct {
	Id string
	KV []StreamEntryValue
}
type StreamStore struct {
	entries []*StreamEntry
	lastId  string
}

func NewStreamStore() *StreamStore {
	return &StreamStore{
		entries: make([]*StreamEntry, 0),
		lastId:  "0-0",
	}
}

func NewStreamEntry(id string, kv []StreamEntryValue) *StreamEntry {
	return &StreamEntry{
		Id: id,
		KV: kv,
	}
}

func (s *StreamStore) ValidateEntryId(id string) (ok bool, err error) {
	mil, seq, _ := SplitIdEntry(id)
	lastMil, lastSeq, _ := SplitIdEntry(s.lastId)
	if mil == 0 && seq == 0 {
		return false, errors.New("ERR The ID specified in XADD must be greater than 0-0")
	} else if mil < lastMil || (mil == lastMil && seq <= lastSeq) {
		return false, errors.New("ERR The ID specified in XADD is equal or smaller than the target stream top item")
	}
	return true, nil
}
