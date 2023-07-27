package s3zip

import (
	"context"
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

	err := Run(context.Background(), &RunInput{
		DryRun:         false,
		S3Bucket:       "s3zip-test",
		S3Uploader:     s3manager.NewUploaderWithClient(s3svc),
		S3ObjectHeader: s3svc,
		Path:           dir,
		ZipDepth:       1,
		OutPrefix:      "out-prefix",
	})
	require.NoError(t, err, "Run failed")
	t.Fatal("Hello")
}
