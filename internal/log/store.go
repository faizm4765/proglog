package log

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"os"
	"sync"
)

const (
	lenWidth = 8
)

type store struct {
	os.File // File is embedded in the store struct, allowing direct access to its methods and properties.
	buf     *bufio.Writer
	mu      sync.Mutex
	size    uint64
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

// Append appends the given byte slice to the store.
// It returns the number of bytes written (n) in this append operation, the position where the record was appended (pos), and any error that occurred during the append operation.
// The method ensures that the store is thread-safe by using a mutex to lock the critical section of code that modifies the store's state.
func (s *store) Append(p []byte) (n uint64, pos uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	pos = s.size

	// first it writes the length of the record (the byte slice p) as an 8-byte unsigned integer in big-endian format to the buffer. This is done using the binary.Write function, which takes care of encoding the length correctly. If there is an error during this write operation, it returns 0 for both n and pos, along with the error.
	if err := binary.Write(s.buf, binary.BigEndian, uint64(len(p))); err != nil {
		return 0, 0, err
	}

	// then it writes the actual record data (the byte slice p) to the buffer using the Write method of the bufio.Writer. If there is an error during this write operation, it returns 0 for both n and pos, along with the error.
	w, err := s.buf.Write(p)
	if err != nil {
		return 0, 0, err
	}

	// finally, it updates the total size of the store by adding the length of the record and the length of the size prefix (lenWidth).
	w += lenWidth
	s.size += uint64(w)
	return uint64(w), pos, nil
}

// Read reads a record from the store at the given position (pos). It returns the record as a byte slice and any error that occurred during the read operation.
func (s *store) Read(pos uint64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.buf.Flush(); err != nil {
		return nil, err
	}

	//  here it fetches the size of the record at the given position, which is stored as an 8-byte unsigned integer (uint64) in big-endian format. The size is read into a byte slice of length lenWidth (which is 8 bytes) using the ReadAt method of the store, starting from the specified position (pos). If there is an error during this read operation, it returns nil and the error.
	size := make([]byte, lenWidth)
	if _, err := s.File.ReadAt(size, int64(pos)); err != nil {
		return nil, err
	}

	// now it fetches the actual record data based on the size obtained in the previous step.
	recordSize := binary.BigEndian.Uint64(size)
	record := make([]byte, recordSize)

	// it reads the record data into a byte slice of length recordSize using the ReadAt method, starting from the position immediately after the size (pos + lenWidth).
	if _, err := s.File.ReadAt(record, int64(pos+lenWidth)); err != nil {
		return nil, err
	}

	return record, nil
}

func (s *store) ReadAt(p []byte, off int64) (n int, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.buf.Flush(); err != nil {
		return 0, err
	}

	fmt.Print("check")
	return s.File.ReadAt(p, off)
}

func (s *store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	err := s.buf.Flush()
	if err != nil {
		return err
	}

	return s.File.Close()
}
