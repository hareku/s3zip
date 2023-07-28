package s3zip

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/stretchr/testify/require"
)

func TestRun(t *testing.T) {
	dir := setupTestDir(t, []testFile{
		{path: "a1.txt", content: "a1"},
		{path: "foo/b1.txt", content: "b1"},
		{path: "foo/b2.txt", content: "b2"},
		{path: "foo/bar/c1.txt", content: "c1"},
		{path: "baz/d1.txt", content: "d1"},
	})

	sess := session.Must(session.NewSession())
	s3svc := s3.New(sess, &aws.Config{
		Endpoint:         aws.String("http://localhost:9000"),
		Region:           aws.String("ap-northeast-1"),
		Credentials:      credentials.NewStaticCredentials("minioadmin", "minioadmin", ""),
		S3ForcePathStyle: aws.Bool(true),
	})

	in := &RunInput{
		S3Bucket:     "s3zip-test",
		S3Uploader:   s3manager.NewUploaderWithClient(s3svc),
		S3Service:    s3svc,
		Path:         dir,
		ZipDepth:     1,
		OutPrefix:    "out-prefix",
		StorageClass: s3.StorageClassStandard,
	}
	require.NoError(t, Run(context.Background(), in))

	require.NoError(t, os.Remove(filepath.Join(dir, "a1.txt")))
	require.NoError(t, Run(context.Background(), in))

	t.Fatal("Hello")
}
