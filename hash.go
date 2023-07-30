package s3zip

import (
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"golang.org/x/mod/sumdb/dirhash"
)

// Hash returns a hash of the given file or directory.
// This only uses the file size and name for performance.
func Hash(name string) (string, error) {
	stat, err := os.Stat(name)
	if err != nil {
		return "", fmt.Errorf("stat: %w", err)
	}

	files := []string{"."}
	if stat.IsDir() {
		files, err = dirhash.DirFiles(name, "")
		if err != nil {
			return "", fmt.Errorf("dir files: %w", err)
		}
	}
	sort.Strings(files)

	h := sha256.New()
	for _, file := range files {
		if strings.Contains(file, "\n") {
			return "", errors.New("filenames with newlines are not supported")
		}
		s, err := os.Stat(filepath.Join(name, file))
		if err != nil {
			return "", err
		}
		if file == "." {
			fmt.Fprintf(h, "%d  %s\n", s.Size(), filepath.Join(name, file))
		} else {
			fmt.Fprintf(h, "%d  %s\n", s.Size(), file)
		}
	}
	return "s3zip:" + base64.StdEncoding.EncodeToString(h.Sum(nil)), nil
}
