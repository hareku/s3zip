package s3zip

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

func LocalObjects(path string, maxDepth int) ([]string, error) {
	if maxDepth == 0 {
		return []string{"."}, nil
	}

	var objects []string
	path = filepath.Clean(path)
	err := filepath.Walk(path, func(file string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		rel, err := filepath.Rel(path, file)
		if err != nil {
			return fmt.Errorf("get relative path: %w", err)
		}

		depth := strings.Count(rel, string(os.PathSeparator)) + 1
		if depth > maxDepth {
			return filepath.SkipDir // no more recursion
		}
		if depth < maxDepth && info.IsDir() {
			return nil // continue recursion
		}
		if rel != "." {
			objects = append(objects, filepath.ToSlash(rel))
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return objects, nil
}
