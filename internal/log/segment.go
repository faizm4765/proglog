package log

import (
	"fmt"
	"os"
	"path"
)

type segment struct {
	store      *store
	index      *Index
	baseOffset int64 // baseOffset is the starting offset of the segment, which is used to calculate the actual offset of records within the segment.
	nextOffset int64 // nextOffset is the offset that will be assigned to the next record appended to the segment.
}

// segment which wraps index and store
func newSegment(dir string, baseOffset int64) (*segment, error) {
	s := &segment{baseOffset: baseOffset}

	var err error
	storeFile, err := os.OpenFile(path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".store")), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	s.store, err = newStore(storeFile)
	if err != nil {
		return nil, err
	}

	//  index file is not opened in append mode because we will be writing to specific positions in the file based on the offset and position of the records, rather than just appending to the end of the file.
	indexFile, err := os.OpenFile(path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".index")), os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}

	// todo: check later on this file size
	var maxIndexBytes uint64 = 1024 * 1024 // 1MB
	s.index, err = newIndex(indexFile, maxIndexBytes)
	if err != nil {
		return nil, err
	}

	// set nextOffset for the next appended record
	if s.index.size == 0 {
		s.nextOffset = baseOffset
	} else {
		numOfEntries := s.index.size / indexRecordWidth
		off, _, _ := s.index.Read(numOfEntries - 1)
		s.nextOffset = baseOffset + int64(off) + 1
	}

	return s, nil
}

// append record to the segment
func (s *segment) Append(record []byte) (offset int64, err error) {
	_, pos, err := s.store.Append(record)
	if err != nil {
		return 0, err
	}

	cur := s.nextOffset
	s.nextOffset++

	index_offset := cur - s.baseOffset
	s.index.Write(uint32(index_offset), uint64(pos))

	return cur, nil
}

// read record from the segment
func (s *segment) Read(offset int64) ([]byte, error) {
	if offset < s.baseOffset || offset >= s.nextOffset {
		return nil, fmt.Errorf("offset out of range")
	}

	// convert given offset to index offset
	index_offset := uint64(offset - s.baseOffset)

	// read the index record to get the position of the record in the store
	_, pos, err := s.index.Read(index_offset)
	if err != nil {
		return nil, err
	}

	// read the record from the store using the position obtained from the index
	record, err := s.store.Read(pos)
	if err != nil {
		return nil, err
	}

	return record, nil
}

func (s *segment) Remove() error {
	if err := s.Close(); err != nil {
		return err
	}

	if err := os.Remove(s.index.Name()); err != nil {
		return err
	}

	if err := os.Remove(s.store.Name()); err != nil {
		return err
	}

	return nil
}

func (s *segment) Close() error {
	if err := s.index.Close(); err != nil {
		return err
	}

	if err := s.store.Close(); err != nil {
		return err
	}

	return nil
}
