package s3zip

import (
	"archive/zip"
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRun(t *testing.T) {
	dir := setupTestDir(t, "target", []testFile{
		{path: "a1.txt", content: "a1"},
		{path: "foo/b1.txt", content: "b1"},
		{path: "foo/b2.txt", content: "b2"},
		{path: "foo/bar/c1.txt", content: "c1"},
		{path: "baz/d1.txt", content: "d1"},
	})

	s3svc := s3.New(session.Must(session.NewSession()), &aws.Config{
		Endpoint:         aws.String("http://localhost:9000"),
		Region:           aws.String("ap-northeast-1"),
		Credentials:      credentials.NewStaticCredentials("minioadmin", "minioadmin", ""),
		S3ForcePathStyle: aws.Bool(true),
	})
	in := &RunInput{
		S3Bucket:       "s3zip-test",
		S3Uploader:     s3manager.NewUploaderWithClient(s3svc),
		S3Service:      s3svc,
		Path:           dir,
		ZipDepth:       1,
		OutPrefix:      "pref",
		S3StorageClass: s3.StorageClassStandard,
	}
	defer func() { // remove all objects in the bucket/prefix
		keys := make([]*s3.ObjectIdentifier, 0)
		require.NoError(t, s3svc.ListObjectsV2Pages(&s3.ListObjectsV2Input{
			Bucket: aws.String(in.S3Bucket),
			Prefix: aws.String(in.OutPrefix),
		}, func(output *s3.ListObjectsV2Output, lastPage bool) bool {
			for _, object := range output.Contents {
				keys = append(keys, &s3.ObjectIdentifier{Key: object.Key})
			}
			return lastPage
		}))
		_, err := s3svc.DeleteObjects(&s3.DeleteObjectsInput{
			Bucket: aws.String(in.S3Bucket),
			Delete: &s3.Delete{
				Objects: keys,
			},
		})
		require.NoError(t, err)
	}()

	assertS3Objects := func(t *testing.T, wantObjects map[string][]testFile) {
		t.Helper()

		got := make([]string, 0)
		require.NoError(t, s3svc.ListObjectsV2Pages(&s3.ListObjectsV2Input{
			Bucket: aws.String(in.S3Bucket),
		}, func(output *s3.ListObjectsV2Output, lastPage bool) bool {
			for _, object := range output.Contents {
				got = append(got, *object.Key)
			}
			return lastPage
		}))
		sort.Strings(got)
		wantKeys := make([]string, 0, len(wantObjects))
		for k := range wantObjects {
			wantKeys = append(wantKeys, k)
		}
		sort.Strings(wantKeys)
		require.Equal(t, wantKeys, got)

		dl := s3manager.NewDownloaderWithClient(s3svc)
		for k, wantFiles := range wantObjects {
			buf := aws.NewWriteAtBuffer([]byte{})
			_, err := dl.Download(buf, &s3.GetObjectInput{
				Bucket: aws.String(in.S3Bucket),
				Key:    aws.String(k),
			})
			require.NoError(t, err)
			zr, err := zip.NewReader(bytes.NewReader(buf.Bytes()), int64(len(buf.Bytes())))
			require.NoError(t, err)
			for _, zf := range zr.File {
				f, err := zf.Open()
				require.NoError(t, err)
				content := make([]byte, zf.UncompressedSize64)
				wrote, err := f.Read(content)
				if err != nil {
					require.ErrorIs(t, err, io.EOF, "Failed to read file %q in %q (full size: %d, wrote: %d)", zf.Name, k, zf.UncompressedSize64, wrote)
				}
				require.NoError(t, f.Close())

				var ok bool
				for _, wantFile := range wantFiles {
					if wantFile.path == zf.Name {
						ok = true
						assert.Equal(t, wantFile.content, string(content))
						break
					}
				}
				assert.True(t, ok, "file %s not found in: %v", zf.Name, wantFiles)
			}
		}
	}

	t.Run("upload", func(t *testing.T) {
		out, err := Run(context.Background(), in)
		require.NoError(t, err)
		assert.Equal(t, &RunOutput{
			Uploaded: 3,
			Deleted:  0,
		}, out)
		assertS3Objects(t, map[string][]testFile{
			"pref/target/a1.txt.zip": {
				{path: "a1.txt", content: "a1"},
			},
			"pref/target/foo.zip": {
				{path: "b1.txt", content: "b1"},
				{path: "b2.txt", content: "b2"},
				{path: "bar/c1.txt", content: "c1"},
			},
			"pref/target/baz.zip": {
				{path: "d1.txt", content: "d1"},
			},
		})
	})

	t.Run("delete", func(t *testing.T) {
		require.NoError(t, os.Remove(filepath.Join(dir, "a1.txt")))
		require.NoError(t, os.RemoveAll(filepath.Join(dir, "baz")))

		out, err := Run(context.Background(), in)
		require.NoError(t, err)
		assert.Equal(t, &RunOutput{
			Uploaded: 0,
			Deleted:  2,
		}, out)
		assertS3Objects(t, map[string][]testFile{
			"pref/target/foo.zip": {
				{path: "b1.txt", content: "b1"},
				{path: "b2.txt", content: "b2"},
				{path: "bar/c1.txt", content: "c1"},
			},
		})
	})

	t.Run("ignore file content", func(t *testing.T) {
		f, err := os.Create(filepath.Join(dir, "foo/b1.txt"))
		require.NoError(t, err)
		_, err = f.Write([]byte("b1-2"))
		require.NoError(t, err)
		require.NoError(t, f.Close())

		out, err := Run(context.Background(), in)
		require.NoError(t, err)
		assert.Equal(t, &RunOutput{
			Uploaded: 0,
			Deleted:  0,
		}, out)
		assertS3Objects(t, map[string][]testFile{
			"pref/target/foo.zip": {
				{path: "b1.txt", content: "b1"},
				{path: "b2.txt", content: "b2"},
				{path: "bar/c1.txt", content: "c1"},
			},
		})
	})

	t.Run("remove a file in zip", func(t *testing.T) {
		require.NoError(t, os.Rename(filepath.Join(dir, "foo/b1.txt"), filepath.Join(dir, "foo/b1-2.txt")))

		out, err := Run(context.Background(), in)
		require.NoError(t, err)
		assert.Equal(t, &RunOutput{
			Uploaded: 1,
			Deleted:  0,
		}, out)
		assertS3Objects(t, map[string][]testFile{
			"pref/target/foo.zip": {
				{path: "b1-2.txt", content: "b1-2"},
				{path: "b2.txt", content: "b2"},
				{path: "bar/c1.txt", content: "c1"},
			},
		})
	})
}
