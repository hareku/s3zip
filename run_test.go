package s3zip

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/stretchr/testify/require"
)

func TestRun(t *testing.T) {
	files := []testFile{
		{path: "a1.txt", content: "a1"},
		{path: "foo/b1.txt", content: "b1"},
		{path: "foo/b2.txt", content: "b2"},
		{path: "foo/bar/c1.txt", content: "c1"},
		{path: "baz/d1.txt", content: "d1"},
	}
	dir := setupTestDir(t, "target", files)

	sess := session.Must(session.NewSession())
	s3svc := s3.New(sess, &aws.Config{
		Endpoint:         aws.String("http://localhost:9000"),
		Region:           aws.String("ap-northeast-1"),
		Credentials:      credentials.NewStaticCredentials("minioadmin", "minioadmin", ""),
		S3ForcePathStyle: aws.Bool(true),
	})
	// downloader := s3manager.NewDownloaderWithClient(s3svc)

	in := &RunInput{
		S3Bucket:     "s3zip-test",
		S3Uploader:   s3manager.NewUploaderWithClient(s3svc),
		S3Service:    s3svc,
		Path:         dir,
		ZipDepth:     1,
		OutPrefix:    "pref",
		StorageClass: s3.StorageClassStandard,
	}
	defer func() { // remove all objects in the bucket/prefix
		keys := make([]*s3.ObjectIdentifier, 0)
		require.NoError(t, s3svc.ListObjectsV2Pages(&s3.ListObjectsV2Input{
			Bucket: aws.String(in.S3Bucket),
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

	require.NoError(t, Run(context.Background(), in))

	assertS3Objects := func(t *testing.T, wantObjects []string) {
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
		sort.Strings(wantObjects)
		require.Equal(t, wantObjects, got)
	}
	assertS3Objects(t, []string{
		"pref/target/a1.txt.zip",
		"pref/target/foo.zip",
		"pref/target/baz.zip",
	})

	require.NoError(t, os.Remove(filepath.Join(dir, "target/a1.txt")))
	require.NoError(t, Run(context.Background(), in))
	assertS3Objects(t, []string{
		"pref/target/foo.zip",
		"pref/target/baz.zip",
	})

	t.Fatal("Hello")
}
