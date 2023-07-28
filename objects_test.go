package s3zip

import (
	"fmt"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLocalObjects(t *testing.T) {
	dir := setupTestDir(t, "", []testFile{
		{path: "a1.txt", content: "a1"},
		{path: "foo/b1.txt", content: "b1"},
		{path: "foo/b2.txt", content: "b2"},
		{path: "foo/bar/c1.txt", content: "c1"},
		{path: "baz/d1.txt", content: "d1"},
	})

	tests := []struct {
		depth int
		want  []string
	}{
		{
			depth: 0,
			want:  []string{"."},
		},
		{
			depth: 1,
			want:  []string{"a1.txt", "foo", "baz"},
		},
		{
			depth: 2,
			want:  []string{"a1.txt", "foo/b1.txt", "foo/b2.txt", "foo/bar", "baz/d1.txt"},
		},
		{
			depth: 3,
			want:  []string{"a1.txt", "foo/b1.txt", "foo/b2.txt", "foo/bar/c1.txt", "baz/d1.txt"},
		},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("depth=%d", tt.depth), func(t *testing.T) {
			got, err := LocalObjects(dir, tt.depth)
			require.NoError(t, err)

			sort.Strings(got)
			sort.Strings(tt.want)
			require.Equal(t, tt.want, got)
		})
	}
}
