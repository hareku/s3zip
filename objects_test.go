package s3zip

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDirObjects(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	testFiles := []struct {
		path    string
		content string
	}{
		{path: "a1.txt", content: "a1"},
		{path: "foo/b1.txt", content: "b1"},
		{path: "foo/b2.txt", content: "b2"},
		{path: "foo/bar/c1.txt", content: "c1"},
		{path: "baz/d1.txt", content: "d1"},
	}
	for _, v := range testFiles {
		require.NoError(t, os.MkdirAll(filepath.Join(tmpDir, filepath.Dir(v.path)), 0755))

		f, err := os.Create(filepath.Join(tmpDir, v.path))
		require.NoError(t, err)
		_, err = f.WriteString(v.content)
		require.NoError(t, err)
		require.NoError(t, f.Close())
	}

	tests := []struct {
		depth int
		want  []string
	}{
		{
			depth: 0,
			want:  []string{"a1.txt", "foo", "baz"},
		},
		{
			depth: 1,
			want:  []string{"a1.txt", "foo/b1.txt", "foo/b2.txt", "foo/bar", "baz/d1.txt"},
		},
		{
			depth: 2,
			want:  []string{"a1.txt", "foo/b1.txt", "foo/b2.txt", "foo/bar/c1.txt", "baz/d1.txt"},
		},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("depth %d", tt.depth), func(t *testing.T) {
			got, err := DirObjects(tmpDir, tt.depth)
			require.NoError(t, err)

			sort.Strings(got)
			sort.Strings(tt.want)
			require.Equal(t, tt.want, got)
		})
	}
}
