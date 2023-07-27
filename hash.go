package s3zip

import (
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
	if stat.IsDir() {
		return dirhash.HashDir(name, "", dirhash.Hash1)
	}
	return dirhash.Hash1([]string{name}, func(s string) (io.ReadCloser, error) {
		return os.Open(s)
	})
}
