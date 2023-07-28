package s3zip

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

type testFile struct {
	path    string
	content string
}

func setupTestDir(t *testing.T, prefix string, files []testFile) string {
	t.Helper()

	tmpDir, err := os.MkdirTemp("", "")
	require.NoError(t, err)
	t.Cleanup(func() {
		os.RemoveAll(tmpDir)
	})
	t.Logf("Created tmp dir: %s", tmpDir)
	if prefix != "" {
		tmpDir = filepath.Join(tmpDir, prefix)
	}

	for _, v := range files {
		require.NoError(t, os.MkdirAll(filepath.Join(tmpDir, filepath.Dir(v.path)), 0755))

		f, err := os.Create(filepath.Join(tmpDir, v.path))
		require.NoError(t, err)
		_, err = f.WriteString(v.content)
		require.NoError(t, err)
		require.NoError(t, f.Close())
	}

	return tmpDir
}
