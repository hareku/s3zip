package s3zip

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

func LocalObjects(path string, maxDepth int) ([]string, error) {
	_, pref := filepath.Split(path)

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
		rel = filepath.Join(pref, rel)

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
