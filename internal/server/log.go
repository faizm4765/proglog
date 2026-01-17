package server

import (
	"fmt"
	"sync"
)

type Record struct {
	Offset int
	Value  []byte
}

type Log struct {
	mu      sync.Mutex
	records []Record
}

var ErrOffsetOutOfRange = fmt.Errorf("Offset not found")

func NewLog() *Log {
	return &Log{}
}

func (l *Log) Append(record Record) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.records = append(l.records, record)
	record.Offset = len(l.records) - 1
	return record.Offset, nil
}

func (l *Log) Read(offset int) (Record, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if offset < 0 || offset >= len(l.records) {
		return Record{}, ErrOffsetOutOfRange
	}

	return l.records[offset], nil
}
