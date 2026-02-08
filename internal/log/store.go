package log

import (
	"bufio"
	"os"
	"sync"
)

type store struct {
	os.File
	buf  *bufio.Writer
	mu   sync.Mutex
	size uint64
}

func newStore(f *os.File) (*store, error) {
	fileInfo, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}

	size := uint64(fileInfo.Size())
	return &store{
		File: *f,
		buf:  bufio.NewWriter(f),
		size: size,
	}, nil
}
