package s3zip

import (
	"fmt"
	"path/filepath"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLocalObjects(t *testing.T) {
	dir := setupTestDir(t, "pref", []testFile{
		{path: "a1.txt", content: "a1"},
		{path: "foo/b1.txt", content: "b1"},
		{path: "foo/b2.txt", content: "b2"},
		{path: "foo/bar/c1.txt", content: "c1"},
		{path: "baz/d1.txt", content: "d1"},
	})

	tests := []struct {
		path  string
		depth int
		want  []string
	}{
		{
			path:  dir,
			depth: 0,
			want:  []string{"pref"},
		},
		{
			path:  dir,
			depth: 1,
			want:  []string{"pref/a1.txt", "pref/foo", "pref/baz"},
		},
		{
			path:  dir,
			depth: 2,
			want:  []string{"pref/a1.txt", "pref/foo/b1.txt", "pref/foo/b2.txt", "pref/foo/bar", "pref/baz/d1.txt"},
		},
		{
			path:  dir,
			depth: 3,
			want:  []string{"pref/a1.txt", "pref/foo/b1.txt", "pref/foo/b2.txt", "pref/foo/bar/c1.txt", "pref/baz/d1.txt"},
		},
		{
			path:  filepath.Join(dir, "a1.txt"),
			depth: 0,
			want:  []string{"a1.txt"},
		},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("path=%q,depth=%d", tt.path, tt.depth), func(t *testing.T) {
			got, err := LocalObjects(tt.path, tt.depth)
			require.NoError(t, err)

			sort.Strings(got)
			sort.Strings(tt.want)
			require.Equal(t, tt.want, got)
		})
	}
}
