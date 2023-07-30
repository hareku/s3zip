package s3zip

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"golang.org/x/exp/slog"
	"golang.org/x/sync/errgroup"
)

const MetadataKeyHash = "S3zip-Hash"

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

// Run zip and upload files in the given path.
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
	{
		eg, ctx := errgroup.WithContext(ctx)
		eg.SetLimit(10)
		var mu sync.Mutex
		for _, object := range objects {
			object := object
			eg.Go(func() error {
				objectHash, err := Hash(filepath.Join(in.Path, object))
				if err != nil {
					return fmt.Errorf("compute hash %q: %w", object, err)
				}

				shouldUpload, err := shouldUpload(ctx, in, object, objectHash)
				if err != nil {
					return fmt.Errorf("check should upload %q: %w", object, err)
				}
				if !shouldUpload {
					return nil
				}

				mu.Lock()
				defer mu.Unlock()
				objectsToUpload = append(objectsToUpload, struct {
					name string
					hash string
				}{
					name: object,
					hash: objectHash,
				})
				return nil
			})
		}
		if err := eg.Wait(); err != nil {
			return nil, fmt.Errorf("check should upload objects: %w", err)
		}
	}

	if !in.DryRun {
		eg, ctx := errgroup.WithContext(ctx)
		eg.SetLimit(5)
		var mu sync.Mutex
		for _, v := range objectsToUpload {
			v := v
			eg.Go(func() error {
				defer func() {
					mu.Lock()
					defer mu.Unlock()
					out.Uploaded++
					slog.InfoContext(ctx, fmt.Sprintf("Uploaded(%d/%d)", out.Uploaded, len(objectsToUpload)))
				}()
				if err := uploadObject(ctx, in, v.name, v.hash); err != nil {
					return fmt.Errorf("upload %q: %w", v.name, err)
				}
				return nil
			})
		}
		if err := eg.Wait(); err != nil {
			return nil, fmt.Errorf("upload objects: %w", err)
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
				slog.InfoContext(ctx, "Upload (new)", "object", object, "s3-key", makeS3Key(in.Path, in.OutPrefix, object))
				return true, nil
			}
		}
		return false, fmt.Errorf("head object: %w", err)
	}

	s3hash, ok := head.Metadata[MetadataKeyHash]
	if !ok {
		return false, fmt.Errorf("missing %s metadata in s3: %+v", MetadataKeyHash, head.Metadata)
	}
	if *s3hash == objectHash {
		slog.InfoContext(ctx, "Skip (uploaded)", "object", object, "s3-key", makeS3Key(in.Path, in.OutPrefix, object))
		return false, nil
	}
	slog.InfoContext(ctx, "Upload (changed)", "object", object, "s3-key", makeS3Key(in.Path, in.OutPrefix, object))
	return true, nil
}

func uploadObject(ctx context.Context, in *RunInput, object, objectHash string) error {
	r := Zip(filepath.Join(in.Path, object))
	defer r.Close()
	_, err := in.S3Uploader.UploadWithContext(ctx, &s3manager.UploadInput{
		Bucket:       &in.S3Bucket,
		Key:          aws.String(makeS3Key(in.Path, in.OutPrefix, object)),
		Body:         r,
		ContentType:  aws.String("application/zip"),
		Metadata:     map[string]*string{MetadataKeyHash: &objectHash},
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
