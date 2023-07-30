package s3zip

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHash(t *testing.T) {
	dir := setupTestDir(t, "target", []testFile{
		{path: "a1.txt", content: "a1"},
		{path: "foo/b1.txt", content: "b1"},
		{path: "foo/b2.txt", content: "b2"},
		{path: "foo/bar/c1.txt", content: "c1"},
		{path: "baz/d1.txt", content: "d1"},
	})

	got, err := Hash(dir)
	require.NoError(t, err)
	got2, err := Hash(dir)
	require.NoError(t, err)
	require.Equal(t, got, got2)

	f, err := os.Create(filepath.Join(dir, "a1.txt"))
	require.NoError(t, err)
	_, err = f.WriteString("aa")
	require.NoError(t, err)
	require.NoError(t, f.Close())

	got3, err := Hash(dir)
	require.NoError(t, err)
	require.Equal(t, got, got3, "Hash should not change if file content is changed but its size is the same")

	require.NoError(t, os.RemoveAll(filepath.Join(dir, "baz")))
	got4, err := Hash(dir)
	require.NoError(t, err)
	require.NotEqual(t, got, got4, "Hash should change if file is removed")
}
