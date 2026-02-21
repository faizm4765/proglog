package log

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

var testData = []byte("Hello, World!")

func TestStoreAppendRead(t *testing.T) {
	tempFile, err := os.CreateTemp("", "store_test")
	require.NoError(t, err)
	defer os.Remove(tempFile.Name())
	s, err := newStore(tempFile)
	require.NoError(t, err)

	testAppend(t, s)
	testRead(t, s)
	testReadAt(t, s)
}

func testAppend(t *testing.T, s *store) {
	t.Helper()
	for i := 0; i < 4; i++ {
		n, pos, err := s.Append(testData)
		require.NoError(t, err)
		require.Equal(t, uint64(len(testData)+lenWidth)*uint64(i+1), pos+n)
	}
}

func testRead(t *testing.T, s *store) {
	t.Helper()
	for i := 0; i < 4; i++ {
		pos := uint64(len(testData)+lenWidth) * uint64(i)
		data, err := s.Read(pos)
		require.NoError(t, err)
		require.Equal(t, testData, data)
	}
}

func testReadAt(t *testing.T, s *store) {
	t.Helper()
	for i := 0; i < 4; i++ {
		buf := make([]byte, len(testData))
		offset_pos := uint64(len(testData)+lenWidth) * uint64(i)
		n, err := s.ReadAt(buf, int64(offset_pos+lenWidth))
		require.NoError(t, err)
		require.Equal(t, len(testData), n)
		require.Equal(t, testData, buf)
	}
}
