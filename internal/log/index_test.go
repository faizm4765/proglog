package log

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIndex(t *testing.T) {
	tempFile, err := ioutil.TempFile(os.TempDir(), "index_test")
	require.NoError(t, err)
	defer os.Remove(tempFile.Name())

	idx, err := newIndex(tempFile)
	require.NoError(t, err)

	require.Equal(t, tempFile.Name(), idx.Name())

	_, _, err = idx.Read(0)
	require.NoError(t, err)

	_, _, err = idx.Read(0)
	require.NoError(t, err)

}

func TestIndexRead(t *testing.T) {
	// this test is to verify that the Read method of the Index struct correctly retrieves the offset and position values from the memory-mapped file. We will create a temporary file, write some index records to it, and then use the Read method to read those records back and verify that they match what we wrote.
}
