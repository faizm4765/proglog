package log

import (
	"fmt"
	"os"
	"path"
)

type segment struct {
	store *store
	index *Index
}

// segment whihch wraps index and store
func newSegment(dir string, baseOffset int64) (*segment, error) {
	s := &segment{}

	var err error

	storeFile, err := os.OpenFile(path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".store")), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	s.store, err = newStore(storeFile)
	if err != nil {
		return nil, err
	}

	indexFile, err := os.OpenFile(path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".index")), os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}

	var maxIndexBytes uint64 = 1024 * 1024 // 1MB
	s.index, err = newIndex(indexFile, maxIndexBytes)
	if err != nil {
		return nil, err
	}

	return s, nil
}
