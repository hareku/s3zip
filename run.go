package s3zip

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"golang.org/x/exp/slog"
)

const MetadataKeyHashBeforeZip = "Hash-Before-Zip"

type (
	S3Uploader interface {
		UploadWithContext(ctx context.Context, input *s3manager.UploadInput, options ...func(*s3manager.Uploader)) (*s3manager.UploadOutput, error)
	}

	S3Service interface {
		HeadObjectWithContext(ctx context.Context, input *s3.HeadObjectInput, options ...request.Option) (*s3.HeadObjectOutput, error)
		ListObjectsV2PagesWithContext(ctx context.Context, input *s3.ListObjectsV2Input, fn func(*s3.ListObjectsV2Output, bool) bool, options ...request.Option) error
		DeleteObjectsWithContext(ctx context.Context, input *s3.DeleteObjectsInput, options ...request.Option) (*s3.DeleteObjectsOutput, error)
	}

	RunInput struct {
		DryRun         bool
		S3Bucket       string
		S3Uploader     S3Uploader
		S3Service      S3Service
		Path           string
		ZipDepth       int
		OutPrefix      string
		S3StorageClass string
	}
)

type RunOutput struct {
	Uploaded int
	Deleted  int
}

func Run(ctx context.Context, in *RunInput) (*RunOutput, error) {
	out := &RunOutput{}
	objects, err := LocalObjects(in.Path, in.ZipDepth)
	if err != nil {
		return nil, fmt.Errorf("get objects in %q: %w", in.Path, err)
	}
	slog.InfoContext(ctx, fmt.Sprintf("Found %d objects in %q", len(objects), in.Path))
	slog.InfoContext(ctx, "Checking whether objects should be uploaded or not")

	objectsToUpload := make([]struct {
		name string
		hash string
	}, 0, len(objects))
	for _, object := range objects {
		objectHash, err := Hash(filepath.Join(in.Path, object))
		if err != nil {
			return nil, fmt.Errorf("compute hash %q: %w", object, err)
		}

		shouldUpload, err := shouldUpload(ctx, in, object, objectHash)
		if err != nil {
			return nil, fmt.Errorf("check should upload %q: %w", object, err)
		}
		if !shouldUpload {
			continue
		}
		objectsToUpload = append(objectsToUpload, struct {
			name string
			hash string
		}{
			name: object,
			hash: objectHash,
		})
	}

	if !in.DryRun {
		for i, v := range objectsToUpload {
			slog.InfoContext(ctx, fmt.Sprintf("Uploading(%d/%d)", i+1, len(objectsToUpload)), "object", v.name)
			if err := uploadObject(ctx, in, v.name, v.hash); err != nil {
				return nil, fmt.Errorf("upload %q: %w", v.name, err)
			}
			out.Uploaded++
		}
	}

	deleted, err := cleanUnusedObjects(ctx, in, objects)
	if err != nil {
		return nil, fmt.Errorf("clean unused objects: %w", err)
	}
	out.Deleted = deleted

	return out, nil
}

func shouldUpload(ctx context.Context, in *RunInput, object, objectHash string) (bool, error) {
	// _, pref := filepath.Split(path)
	head, err := in.S3Service.HeadObjectWithContext(ctx, &s3.HeadObjectInput{
		Bucket: &in.S3Bucket,
		Key:    aws.String(makeS3Key(in.Path, in.OutPrefix, object)),
	})
	if err != nil {
		var aerr awserr.Error
		if errors.As(err, &aerr) {
			switch aerr.Code() {
			case s3.ErrCodeNoSuchBucket:
				return false, fmt.Errorf("bucket %q does not exist", in.S3Bucket)
			case s3.ErrCodeNoSuchKey, "NotFound":
				slog.InfoContext(ctx, "Upload (new)", "object", object)
				return true, nil
			}
		}
		return false, fmt.Errorf("head object: %w", err)
	}

	s3hash, ok := head.Metadata[MetadataKeyHashBeforeZip]
	if !ok {
		return false, fmt.Errorf("missing %s metadata in s3: %+v", MetadataKeyHashBeforeZip, head.Metadata)
	}
	if *s3hash == objectHash {
		slog.InfoContext(ctx, "Skip (uploaded)", "object", object)
		return false, nil
	}
	slog.InfoContext(ctx, "Upload (changed)", "object", object)
	return true, nil
}

func uploadObject(ctx context.Context, in *RunInput, object, objectHash string) error {
	r := Zip(filepath.Join(in.Path, object))
	defer r.Close()
	_, err := in.S3Uploader.UploadWithContext(ctx, &s3manager.UploadInput{
		Bucket:       &in.S3Bucket,
		Key:          aws.String(makeS3Key(in.Path, in.OutPrefix, object)),
		Body:         r,
		Metadata:     map[string]*string{MetadataKeyHashBeforeZip: &objectHash},
		StorageClass: &in.S3StorageClass,
	})
	if err != nil {
		return fmt.Errorf("upload to s3: %w", err)
	}
	return nil
}

func cleanUnusedObjects(ctx context.Context, in *RunInput, objects []string) (int, error) {
	mp := make(map[string]struct{})
	for _, v := range objects {
		mp[makeS3Key(in.Path, in.OutPrefix, v)] = struct{}{}
	}

	dels := make([]*s3.ObjectIdentifier, 0)
	err := in.S3Service.ListObjectsV2PagesWithContext(ctx, &s3.ListObjectsV2Input{
		Bucket: &in.S3Bucket,
		Prefix: &in.OutPrefix,
	}, func(page *s3.ListObjectsV2Output, lastPage bool) bool {
		for _, obj := range page.Contents {
			if _, ok := mp[*obj.Key]; !ok {
				slog.InfoContext(ctx, "Delete", "s3-key", *obj.Key)
				dels = append(dels, &s3.ObjectIdentifier{
					Key: obj.Key,
				})
			}
		}
		return lastPage
	})
	if err != nil {
		return 0, fmt.Errorf("list objects: %w", err)
	}
	if len(dels) == 0 {
		return 0, nil
	}
	if in.DryRun {
		return 0, nil
	}
	_, err = in.S3Service.DeleteObjectsWithContext(ctx, &s3.DeleteObjectsInput{
		Bucket: &in.S3Bucket,
		Delete: &s3.Delete{
			Objects: dels,
		},
	})
	if err != nil {
		return 0, fmt.Errorf("delete objects: %w", err)
	}
	return len(dels), nil
}

func makeS3Key(localPath, outPrefix, object string) string {
	return filepath.ToSlash(filepath.Join(outPrefix, filepath.Base(localPath), object)) + ".zip"
}
