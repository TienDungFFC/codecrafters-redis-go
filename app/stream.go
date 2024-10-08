package main

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

var stream = make(map[string]*StreamStore)

type StreamEntryValue struct {
	Key   string
	Value string
}

type EntryId struct {
	timestamp int64
	seq       int
}

type StreamEntry struct {
	Id *EntryId
	KV []StreamEntryValue
}
type StreamStore struct {
	entries []*StreamEntry
	lastId  *EntryId
}

func NewStreamStore() *StreamStore {
	return &StreamStore{
		entries: make([]*StreamEntry, 0),
		lastId:  nil,
	}
}

func NewStreamEntry(id EntryId, kv []StreamEntryValue) *StreamEntry {
	return &StreamEntry{
		Id: &id,
		KV: kv,
	}
}

func (s *StreamStore) ValidateEntryId(id string) (ok bool, err error) {
	if id == "*" {
		return true, nil
	}
	ids := strings.Split(id, "-")
	var lastMil int64 = 0
	lastSeq := 0

	if s != nil && s.lastId != nil {
		lastMil = s.lastId.timestamp
		lastSeq = s.lastId.seq
	}
	if ids[1] != "*" {

		mil, seq := ConverIdEntryInt(ids)
		if mil == 0 && seq == 0 {
			return false, errors.New("ERR The ID specified in XADD must be greater than 0-0")
		} else if mil < lastMil || (mil == lastMil && seq <= lastSeq) {
			return false, errors.New("ERR The ID specified in XADD is equal or smaller than the target stream top item")
		}
	}

	return true, nil
}

func (s *StreamStore) FindEntryId(t int64) (eId EntryId, ok bool) {
	eId = EntryId{
		timestamp: t,
		seq:       0,
	}
	ok = false
	for _, entry := range s.entries {
		if entry.Id.timestamp == t {
			ok = true
			eId = *entry.Id
			break
		}
	}
	return
}

func (s *StreamStore) EntryIdToString(id EntryId) string {
	ts := strconv.FormatInt(id.timestamp, 10)
	ses := strconv.Itoa(id.seq)
	return fmt.Sprintf("%s-%s", ts, ses)
}

func (e *StreamEntry) CheckRangeQuery(sEntry EntryId, eEntry EntryId, rte bool) bool {
	if rte && e.Id.timestamp+int64(e.Id.seq) >= sEntry.timestamp+int64(sEntry.seq) {
		return true
	} else if e.Id.timestamp+int64(e.Id.seq) >= sEntry.timestamp+int64(sEntry.seq) && e.Id.timestamp+int64(e.Id.seq) <= eEntry.timestamp+int64(eEntry.seq) {
		return true
	}
	return false
}
