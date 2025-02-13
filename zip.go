package s3zip

import (
	"archive/zip"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// Zip creates a zip file from the given directory.
func Zip(name string) io.ReadCloser {
	pr, pw := io.Pipe()
	go func() {
		zw := zip.NewWriter(pw)
		defer zw.Close()

		err := filepath.Walk(name, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if info.IsDir() {
				return nil
			}

			rel, err := filepath.Rel(name, path)
			if err != nil {
				return fmt.Errorf("get relative path: %w", err)
			}
			if rel == "." {
				rel = filepath.Base(name)
			}

			zw2, err := zw.CreateHeader(&zip.FileHeader{
				Name:   filepath.ToSlash(rel),
				Method: zip.Deflate,
			})
			if err != nil {
				return fmt.Errorf("create zip file: %w", err)
			}
			f, err := os.Open(path)
			if err != nil {
				return fmt.Errorf("open file: %w", err)
			}
			defer f.Close()
			if _, err := io.Copy(zw2, f); err != nil {
				return fmt.Errorf("copy file: %w", err)
			}
			return nil
		})
		if err != nil {
			pw.CloseWithError(fmt.Errorf("walk: %w", err))
			return
		}
		if err := zw.Close(); err != nil {
			pw.CloseWithError(fmt.Errorf("close zip: %w", err))
			return
		}
		pw.Close()
	}()

	return pr
}
