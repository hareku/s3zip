package s3zip

import (
	"io"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestZip(t *testing.T) {
	dir := setupTestDir(t, "", []testFile{
		{path: "a.txt", content: "a"},
		{path: "b.txt", content: "b"},
	})

	t.Run("file", func(t *testing.T) {
		r := Zip(filepath.Join(dir, "a.txt"))
		defer r.Close()
		written, err := io.Copy(io.Discard, r)
		require.NoError(t, err)
		assert.Greater(t, written, int64(0))
	})

	t.Run("directory", func(t *testing.T) {
		r := Zip(dir)
		defer r.Close()
		written, err := io.Copy(io.Discard, r)
		require.NoError(t, err)
		assert.Greater(t, written, int64(0))
	})
}
