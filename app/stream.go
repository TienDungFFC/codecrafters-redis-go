package main

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
}

func NewStreamStore() *StreamStore {
	return &StreamStore{
		entries: make([]*StreamEntry, 0),
	}
}

func NewStreamEntry(id string, kv []StreamEntryValue) *StreamEntry {
	return &StreamEntry{
		Id: id,
		KV: kv,
	}
}
