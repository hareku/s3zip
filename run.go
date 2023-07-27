package s3zip

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"golang.org/x/exp/slog"
	"golang.org/x/mod/sumdb/dirhash"
)

const MetadataKeyHashBeforeZip = "X-Hash-Before-Zip"

type S3Uploader interface {
	UploadWithContext(ctx context.Context, input *s3manager.UploadInput, options ...func(*s3manager.Uploader)) (*s3manager.UploadOutput, error)
}

type S3ObjectHeader interface {
	HeadObjectWithContext(ctx context.Context, input *s3.HeadObjectInput) (*s3.HeadObjectOutput, error)
}

type RunInput struct {
	S3Bucket       string
	S3Uploader     S3Uploader
	S3ObjectHeader S3ObjectHeader
	Targets        []ConfigTarget
}

func Run(ctx context.Context, in *RunInput) error {
	for _, t := range in.Targets {
		objects, err := DirObjects(t.Path, t.ZipDepth)
		if err != nil {
			return fmt.Errorf("get objects: %w", err)
		}
		slog.InfoContext(ctx, "Found %d objects in %s", len(objects), t.Path)

		for _, object := range objects {
			hash, err := dirhash.HashDir(object, t.Path, dirhash.Hash1)
			if err != nil {
				return fmt.Errorf("computed hash dir: %w", err)
			}
			slog.InfoContext(ctx, "Hash %s: %s", object, hash)

			s3Key := filepath.ToSlash(filepath.Join(t.OutPrefix, object))
			head, err := in.S3ObjectHeader.HeadObjectWithContext(ctx, &s3.HeadObjectInput{
				Bucket: &in.S3Bucket,
				Key:    &s3Key,
			})
			if err != nil {
				return fmt.Errorf("head object %q: %w", s3Key, err)
			}

			s3hash, ok := head.Metadata[MetadataKeyHashBeforeZip]
			if !ok {
				return fmt.Errorf("missing %s metadata in %q", MetadataKeyHashBeforeZip, s3Key)
			}
			if *s3hash == hash {
				slog.InfoContext(ctx, "Already uploaded %q", object)
				continue
			}

			slog.InfoContext(ctx, "Upload %q", object)
			r := Zip(filepath.Join(t.Path, object))
			_, err = in.S3Uploader.UploadWithContext(ctx, &s3manager.UploadInput{
				Bucket:   &in.S3Bucket,
				Key:      &object,
				Body:     r,
				Metadata: map[string]*string{MetadataKeyHashBeforeZip: &hash},
			})
			if err != nil {
				return fmt.Errorf("upload %q: %w", object, err)
			}
		}

	}
	return nil
}
