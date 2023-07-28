package s3zip

import (
	"bytes"
	"fmt"
	"io"
	"os"

	"golang.org/x/mod/sumdb/dirhash"
)

func Hash(name string) (string, error) {
	stat, err := os.Stat(name)
	if err != nil {
		return "", fmt.Errorf("stat: %w", err)
	}

	files := []string{name}
	if stat.IsDir() {
		files, err = dirhash.DirFiles(name, "")
		if err != nil {
			return "", fmt.Errorf("dir files: %w", err)
		}
	}
	return dirhash.Hash1(files, func(s string) (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(nil)), nil // don't read file contents for performance
	})
}
