package log

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// cover below test cases for segment
// 1. TestNewSegment
// 2. TestSegmentAppend
// 3. TestSegmentRead
// 4. TestSegmentClose
// 5. TestSegmentRemove

func TestNewSegment(t *testing.T) {
	// create a new segment
	seg, err := newSegment(t.TempDir(), 0)
	require.NoError(t, err)
	require.NotNil(t, seg)
	require.Equal(t, int64(0), seg.baseOffset)
	require.Equal(t, int64(0), seg.nextOffset)

	// create a new segment with non-zero base offset
	seg, err = newSegment(t.TempDir(), 10)
	require.NoError(t, err)
	require.NotNil(t, seg)
	require.Equal(t, int64(10), seg.baseOffset)
	require.Equal(t, int64(10), seg.nextOffset)
}

func TestSegmentAppend(t *testing.T) {
	// create a new segment
	seg, err := newSegment(t.TempDir(), 0)
	require.NoError(t, err)
	require.NotNil(t, seg)
	require.Equal(t, int64(0), seg.baseOffset)
	require.Equal(t, int64(0), seg.nextOffset)

	// append a record to the segment
	offset, err := seg.Append([]byte("Hello World"))
	require.NoError(t, err)
	require.Equal(t, int64(0), offset)
	require.Equal(t, int64(1), seg.nextOffset)

	// append another record to the segment
	offset, err = seg.Append([]byte("Hello Again"))
	require.NoError(t, err)
	require.Equal(t, int64(1), offset)
	require.Equal(t, int64(2), seg.nextOffset)

	// close the segment
	err = seg.Close()
	require.NoError(t, err)

	// append a record to the segment with non-zero base offset
	seg, err = newSegment(t.TempDir(), 10)
	require.NoError(t, err)
	require.NotNil(t, seg)
	require.Equal(t, int64(10), seg.baseOffset)
	require.Equal(t, int64(10), seg.nextOffset)
}

func TestSegmentRead(t *testing.T) {
	// create a new segment
	seg, err := newSegment(t.TempDir(), 0)
	require.NoError(t, err)
	require.NotNil(t, seg)

	// append a record to the segment
	offset, err := seg.Append([]byte("Hello World"))
	require.NoError(t, err)
	require.Equal(t, int64(0), offset)

	// read the record from the segment
	record, err := seg.Read(0)
	require.NoError(t, err)
	require.Equal(t, []byte("Hello World"), record)
}

func TestSegmentScenarios(t *testing.T) {
	// create a new segment
	dir := t.TempDir()
	seg, err := newSegment(dir, 16)
	require.NoError(t, err)
	require.NotNil(t, seg)
	require.Equal(t, int64(16), seg.baseOffset)
	require.Equal(t, int64(16), seg.nextOffset)

	// append a record to the segment
	offset, err := seg.Append([]byte("Hello"))
	require.NoError(t, err)
	require.Equal(t, int64(16), offset)
	require.Equal(t, int64(17), seg.nextOffset)

	// append another record to the segment
	offset, err = seg.Append([]byte("World"))
	require.NoError(t, err)
	require.Equal(t, int64(17), offset)
	require.Equal(t, int64(18), seg.nextOffset)

	// Append "distributed-log" to the segment
	offset, err = seg.Append([]byte("distributed-log"))
	require.NoError(t, err)
	require.Equal(t, int64(18), offset)
	require.Equal(t, int64(19), seg.nextOffset)

	// read the first record from the segment
	record, err := seg.Read(17)
	require.NoError(t, err)
	require.Equal(t, []byte("World"), record)

	// attempt to read a record with an offset that is out of range should return an error
	record, err = seg.Read(15)
	require.Error(t, err)

	// attempt to read a record with an offset that is out of range should return an error
	record, err = seg.Read(19)
	require.Error(t, err)

	//  Close and reopen (index size != 0)
	err = seg.Close()
	require.NoError(t, err)

	// After closing, the index file is truncated to actual size: 3 entries × 12 bytes = 36 bytes
	seg, err = newSegment(dir, 16)
	require.NoError(t, err)
	require.NotNil(t, seg)
	require.Equal(t, int64(16), seg.baseOffset)
	require.Equal(t, int64(19), seg.nextOffset)

	// append after reopening should continue from the last offset
	offset, err = seg.Append([]byte("again"))
	require.NoError(t, err)
	require.Equal(t, int64(19), offset)
	require.Equal(t, int64(20), seg.nextOffset)

}

func TestSegmentClose(t *testing.T) {
	// create a new segment
	seg, err := newSegment(t.TempDir(), 0)
	require.NoError(t, err)
	require.NotNil(t, seg)

	// close the segment
	err = seg.Close()
	require.NoError(t, err)

	// close the segment again should return an error
	err = seg.Close()
	require.Error(t, err)
}

func TestSegmentRemove(t *testing.T) {
	// create a new segment
	seg, err := newSegment(t.TempDir(), 0)
	require.NoError(t, err)
	require.NotNil(t, seg)

	// remove the segment
	err = seg.Remove()
	require.NoError(t, err)
}
