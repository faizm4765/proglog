package log

import (
	"encoding/binary"
	"os"

	"github.com/tysonmote/gommap"
)

var (
	offsetWidth      uint64 = 4
	positionWidth    uint64 = 8
	indexRecordWidth        = offsetWidth + positionWidth
)

type Index struct {
	file *os.File
	mmap gommap.MMap
	size uint64
}

func newIndex(f *os.File, maxBytes uint64) (*Index, error) {
	idx := &Index{
		file: f,
	}

	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}

	idx.size = uint64(fi.Size())

	// expanding the file before mmap is required when you plan to write new data beyond the current file size.
	// maxBytes determines how much space is pre-allocated for the index file.
	if err = os.Truncate(f.Name(), int64(maxBytes)); err != nil {
		return nil, err
	}

	// here we are mapping the file into memory with read and write permissions, and using shared mapping because we want changes to be visible to other processes that might be accessing the same file.
	if idx.mmap, err = gommap.Map(idx.file.Fd(), gommap.PROT_READ|gommap.PROT_WRITE, gommap.MAP_SHARED); err != nil {
		return nil, err
	}

	return idx, nil
}

func (idx *Index) Close() error {
	// mmap.Sync() = flush the content (your index records)
	if err := idx.mmap.Sync(gommap.MS_SYNC); err != nil {
		return err
	}

	// file.Sync() = flush the file's metadata and content to stable storage, ensuring that all changes are written to disk. This is important to guarantee that the data in the file is not lost in case of a crash or power failure.
	if err := idx.file.Sync(); err != nil {
		return err
	}

	if err := idx.file.Truncate(int64(idx.size)); err != nil {
		return err
	}

	if err := idx.mmap.UnsafeUnmap(); err != nil {
		return err
	}
	return idx.file.Close()

}

// here "out" and "pos" refer to the offset and position values that are stored in the index record.
//
// here "in" is the input parameter that specifies which index record we want to read. The method calculates the byte offset of the desired index record in the memory-mapped file and retrieves the offset and position values from that record, returning them as out and pos respectively.
func (idx *Index) Read(in uint64) (out uint32, pos uint64, err error) {
	if idx.size == 0 {
		return 0, 0, nil
	}

	// byte offset of the entry inside the index table is calculated by multiplying the input index (in) by the width of each index record (indexRecordWidth). This allows us to directly access the correct position in the memory-mapped file where the desired index record is located.
	pos = uint64(in * indexRecordWidth)

	// the check pos+indexRecordWidth > idx.size is performed to ensure that we do not attempt to read beyond the end of the memory-mapped file. This is because we did truncate the file to a certain size, and if we try to read beyond that size, we would be accessing memory that is not part of the file, which could lead to undefined behavior or a crash.
	if pos+indexRecordWidth > idx.size {
		return 0, 0, os.ErrInvalid
	}

	// we are reading till end because we want to read the entire index record, which consists of both the offset and the position. The offset is stored in the first 4 bytes of the index record, and the position is stored in the next 8 bytes. By reading until the end of the index record, we ensure that we retrieve both pieces of information correctly.
	out = binary.BigEndian.Uint32(idx.mmap[pos : pos+uint64(offsetWidth)])
	pos = binary.BigEndian.Uint64(idx.mmap[pos+uint64(offsetWidth) : pos+uint64(indexRecordWidth)])

	return out, pos, nil
}

// here we intend to append a new index record to the end of the index file.
func (idx *Index) Write(off uint32, pos uint64) error {
	if idx.size+indexRecordWidth > uint64(len(idx.mmap)) {
		// mmap is full and we cannot write more data to it, so we return an error indicating that the operation is invalid.
		return os.ErrInvalid
	}

	binary.BigEndian.PutUint32(idx.mmap[idx.size:idx.size+uint64(offsetWidth)], off)
	binary.BigEndian.PutUint64(idx.mmap[idx.size+uint64(offsetWidth):idx.size+uint64(indexRecordWidth)], pos)
	idx.size += indexRecordWidth

	return nil
}

func (idx *Index) Name() string {
	return idx.file.Name()
}
