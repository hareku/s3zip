package s3zip

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSize(t *testing.T) {
	dir := setupTestDir(t, "", []testFile{
		{path: "a1.txt", content: "1"},
		{path: "b1.txt", content: "23"},
	})

	got, err := Size(dir)
	require.NoError(t, err, "get directory size")
	assert.Equal(t, 3, got, "directory size mismatch")

	got2, err := Size(filepath.Join(dir, "a1.txt"))
	require.NoError(t, err, "get file size")
	assert.Equal(t, 1, got2, "file size mismatch")
}
