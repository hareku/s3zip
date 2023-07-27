package s3zip

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

func DirObjects(dir string, maxDepth int) ([]string, error) {
	var objects []string
	dir = filepath.Clean(dir)
	err := filepath.Walk(dir, func(file string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && file == dir {
			return fmt.Errorf("%s is not a directory", dir)
		}

		rel, err := filepath.Rel(dir, file)
		if err != nil {
			return fmt.Errorf("get relative path: %w", err)
		}

		depth := strings.Count(rel, string(os.PathSeparator))
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
