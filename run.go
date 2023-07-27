package s3zip

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"golang.org/x/exp/slog"
)

const MetadataKeyHashBeforeZip = "Hash-Before-Zip"

type S3Uploader interface {
	UploadWithContext(ctx context.Context, input *s3manager.UploadInput, options ...func(*s3manager.Uploader)) (*s3manager.UploadOutput, error)
}

type S3ObjectHeader interface {
	HeadObjectWithContext(ctx context.Context, input *s3.HeadObjectInput, options ...request.Option) (*s3.HeadObjectOutput, error)
}

type RunInput struct {
	DryRun         bool
	S3Bucket       string
	S3Uploader     S3Uploader
	S3ObjectHeader S3ObjectHeader
	Path           string
	ZipDepth       int
	OutPrefix      string
}

func Run(ctx context.Context, in *RunInput) error {
	objects, err := DirObjects(in.Path, in.ZipDepth)
	if err != nil {
		return fmt.Errorf("get objects: %w", err)
	}
	slog.InfoContext(ctx, fmt.Sprintf("Found %d objects in %s", len(objects), in.Path))

	for _, object := range objects {
		if err := runObject(ctx, in, object); err != nil {
			return fmt.Errorf("object %q: %w", object, err)
		}
	}
	return nil
}

func runObject(ctx context.Context, in *RunInput, object string) error {
	hash, err := Hash(in.Path, object)
	if err != nil {
		return fmt.Errorf("compute hash: %w", err)
	}

	s3Key := filepath.ToSlash(filepath.Join(in.OutPrefix, object)) + ".zip"
	head, err := in.S3ObjectHeader.HeadObjectWithContext(ctx, &s3.HeadObjectInput{
		Bucket: &in.S3Bucket,
		Key:    &s3Key,
	})
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case s3.ErrCodeNoSuchBucket:
				return fmt.Errorf("bucket %q does not exist", in.S3Bucket)
			case s3.ErrCodeNoSuchKey:
				slog.InfoContext(ctx, "Upload", "object", object, "current_hash", hash)
			}
		} else {
			return fmt.Errorf("head object: %w", err)
		}
	} else {
		s3hash, ok := head.Metadata[MetadataKeyHashBeforeZip]
		if !ok {
			return fmt.Errorf("missing %s metadata in %q: %+v", MetadataKeyHashBeforeZip, s3Key, head.Metadata)
		}
		if *s3hash == hash {
			slog.InfoContext(ctx, "Already uploaded", "object", object)
			return nil
		}
		slog.InfoContext(ctx, "Upload", "object", object, "uploaded_hash", *s3hash, "current_hash", hash)
	}

	if in.DryRun {
		return nil
	}

	r := Zip(filepath.Join(in.Path, object))
	defer r.Close()
	_, err = in.S3Uploader.UploadWithContext(ctx, &s3manager.UploadInput{
		Bucket:   &in.S3Bucket,
		Key:      &s3Key,
		Body:     r,
		Metadata: map[string]*string{MetadataKeyHashBeforeZip: &hash},
	})
	if err != nil {
		return fmt.Errorf("upload to s3: %w", err)
	}
	return nil
}
