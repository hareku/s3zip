package s3zip

import (
	"fmt"
	"os"
	"path/filepath"

	"golang.org/x/mod/sumdb/dirhash"
)

// Size returns the size of the given file or directory.
func Size(name string) (int, error) {
	stat, err := os.Stat(name)
	if err != nil {
		return 0, fmt.Errorf("stat: %w", err)
	}

	files := []string{"."}
	if stat.IsDir() {
		files, err = dirhash.DirFiles(name, "")
		if err != nil {
			return 0, fmt.Errorf("dir files: %w", err)
		}
	}

	var size int
	for _, file := range files {
		s, err := os.Stat(filepath.Join(name, file))
		if err != nil {
			return 0, err
		}
		size += int(s.Size())
	}

	return size, nil
}
