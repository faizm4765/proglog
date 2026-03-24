package log

import (
	"io"
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIndex(t *testing.T) {
	tempFile, err := ioutil.TempFile(os.TempDir(), "index_test")
	require.NoError(t, err)
	defer os.Remove(tempFile.Name())

	idx, err := newIndex(tempFile, 1024)
	require.NoError(t, err)

	require.Equal(t, tempFile.Name(), idx.Name())

	_, _, err = idx.Read(0)
	require.NoError(t, err)
}

func TestIndexRead(t *testing.T) {
	// this test is to verify that the Read method of the Index struct correctly retrieves the offset and position values from the memory-mapped file. We will create a temporary file, write some index records to it, and then use the Read method to read those records back and verify that they match what we wrote.

	tempFile, err := ioutil.TempFile(os.TempDir(), "index_test")
	require.NoError(t, err)
	defer os.Remove(tempFile.Name())

	//  passing 1024 as the maxBytes parameter to newIndex means that we are pre-allocating 1024 bytes for the index file as we cannot write beyond the current file size without expanding it first. This allows us to write index records to the file without having to worry about running out of space, as we have already allocated enough space for our test records.
	idx, err := newIndex(tempFile, 1024)
	require.NoError(t, err)

	// Write some index records to the file
	records := []struct {
		offset   uint32
		position uint64
	}{
		{offset: 1, position: 100},
		{offset: 2, position: 200},
		{offset: 3, position: 300},
	}

	for _, record := range records {
		err := idx.Write(record.offset, record.position)
		require.NoError(t, err)
	}

	for i, record := range records {
		offset, position, err := idx.Read(uint64(i))
		require.NoError(t, err)
		require.Equal(t, record.offset, offset)
		require.Equal(t, record.position, position)
	}

	// reading beyond the last record should return an error, as there are only 3 records (0, 1, 2) and we are trying to read the 4th record (index 3).
	_, _, err = idx.Read(3)
	require.Error(t, err, io.EOF)

	idx.Close()

	f, err := os.OpenFile(tempFile.Name(), os.O_RDWR, 0600)
	require.NoError(t, err)
	defer f.Close()

	// todo: after closing the index, we should not be able to read from it, and it should return an error indicating that the file is closed.
	// _, _, err = idx.Read(0)
	// require.Error(t, err)

	idx, err = newIndex(f, 1024)
	require.NoError(t, err)

	off, pos, err := idx.Read(0)
	require.NoError(t, err)
	require.Equal(t, uint32(records[0].offset), off)
	require.Equal(t, uint64(records[0].position), pos)
}
