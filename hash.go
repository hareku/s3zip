package s3zip

import (
	"fmt"
	"io"
	"os"
	"path/filepath"

	"golang.org/x/mod/sumdb/dirhash"
)

func Hash(path, object string) (string, error) {
	name := filepath.Join(path, object)

	stat, err := os.Stat(name)
	if err != nil {
		return "", fmt.Errorf("stat: %w", err)
	}
	if stat.IsDir() {
		return dirhash.HashDir(name, "", dirhash.Hash1)
	}
	return dirhash.Hash1([]string{object}, func(s string) (io.ReadCloser, error) {
		return os.Open(name)
	})
}
