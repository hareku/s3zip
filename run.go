package s3zip

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"sync"
	"time"

	"log/slog"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"
)

type (
	RunInput struct {
		DryRun           bool
		S3Bucket         string
		S3Uploader       *s3manager.Uploader
		S3Service        *s3.S3
		MetadataStoreKey string
		Path             string
		MaxZipDepth      int
		OutPrefix        string
		S3StorageClass   string
	}
)

type RunOutput struct {
	Upload int
	Delete int
}

type ObjectToUpload struct {
	Name string
	Hash string
}

// Run zip and upload files in the given path.
func Run(ctx context.Context, in *RunInput) (*RunOutput, error) {
	if in.MetadataStoreKey == "" {
		return nil, fmt.Errorf("metadata store key is required")
	}

	objects, err := LocalObjects(in.Path, in.MaxZipDepth)
	if err != nil {
		return nil, fmt.Errorf("get objects in %q: %w", in.Path, err)
	}
	slog.InfoContext(ctx, "Listed objects", "len", len(objects))

	metadataStore, err := loadMetadataStore(ctx, in)
	if err != nil {
		return nil, fmt.Errorf("load metadata store: %w", err)
	}

	if !in.DryRun {
		defer func() {
			timeout := 10 * time.Second
			slog.InfoContext(ctx, "Saving metadata store", "timeout", timeout)

			ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), timeout)
			defer cancel()
			if err := saveMetadataStore(ctx, in, metadataStore); err != nil {
				slog.ErrorContext(ctx, "save metadata store", "error", err)
			}
		}()
	}

	slog.InfoContext(ctx, "Checking whether objects should be uploaded or not")
	objectsToUpload := make([]ObjectToUpload, 0, len(objects))
	{
		eg, ctx := errgroup.WithContext(ctx)
		eg.SetLimit(10)
		var mu sync.Mutex
		for _, object := range objects {
			eg.Go(func() error {
				if ctx.Err() != nil {
					return ctx.Err()
				}

				objectHash, err := Hash(filepath.Join(in.Path, object))
				if err != nil {
					return fmt.Errorf("compute hash %q: %w", object, err)
				}

				mu.Lock()
				defer mu.Unlock()

				key := makeS3Key(in.Path, in.OutPrefix, object)
				if m, ok := metadataStore.Metadata[key]; ok && m.Hash == objectHash {
					return nil
				}

				objectsToUpload = append(objectsToUpload, ObjectToUpload{
					Name: object,
					Hash: objectHash,
				})
				return nil
			})
		}
		if err := eg.Wait(); err != nil {
			return nil, fmt.Errorf("check should upload objects: %w", err)
		}
	}
	slog.InfoContext(ctx, "Found objects to upload", "len", len(objectsToUpload))

	if !in.DryRun {
		var mu sync.Mutex
		for _, v := range objectsToUpload {
			if err := uploadObject(ctx, in, v); err != nil {
				return nil, fmt.Errorf("upload %q: %w", v.Name, err)
			}

			mu.Lock()
			metadataStore.Metadata[makeS3Key(in.Path, in.OutPrefix, v.Name)] = &Metadata{
				Hash: v.Hash,
			}
			mu.Unlock()
		}
	}

	deleted, err := cleanUnusedObjects(ctx, in, objects)
	if err != nil {
		return nil, fmt.Errorf("clean unused objects: %w", err)
	}
	return &RunOutput{
		Upload: len(objectsToUpload),
		Delete: deleted,
	}, nil
}

func loadMetadataStore(ctx context.Context, in *RunInput) (*MetadataStore, error) {
	slog.InfoContext(ctx, "Loading metadata store", "key", in.MetadataStoreKey)

	out, err := in.S3Service.GetObjectWithContext(ctx, &s3.GetObjectInput{
		Bucket: &in.S3Bucket,
		Key:    aws.String(in.MetadataStoreKey),
	})
	if err != nil {
		// return empty metadata store if not found
		var aerr awserr.Error
		if errors.As(err, &aerr) && aerr.Code() == s3.ErrCodeNoSuchKey {
			slog.InfoContext(ctx, "Metadata store not found, creating a new one")
			return &MetadataStore{
				Metadata: make(map[string]*Metadata),
			}, nil
		}

		return nil, fmt.Errorf("get metadata from s3: %w", err)
	}

	b, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}

	var s MetadataStore
	if err := proto.Unmarshal(b, &s); err != nil {
		return nil, fmt.Errorf("unmarshal: %w", err)
	}
	return &s, nil
}

func saveMetadataStore(ctx context.Context, in *RunInput, s *MetadataStore) error {
	b, err := proto.Marshal(s)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}

	_, err = in.S3Uploader.UploadWithContext(ctx, &s3manager.UploadInput{
		Bucket:       &in.S3Bucket,
		Key:          aws.String(in.MetadataStoreKey),
		Body:         aws.ReadSeekCloser(io.NopCloser(bytes.NewReader(b))),
		ContentType:  aws.String("application/protobuf"),
		StorageClass: aws.String(s3.StorageClassStandard),
	})
	if err != nil {
		return fmt.Errorf("put object: %w", err)
	}
	return nil
}

func uploadObject(ctx context.Context, in *RunInput, object ObjectToUpload) error {
	slog.InfoContext(ctx, "Zipping", "object", object)
	r := Zip(filepath.Join(in.Path, object.Name))
	defer r.Close()

	key := makeS3Key(in.Path, in.OutPrefix, object.Name)
	upIn := &s3manager.UploadInput{
		Bucket:       &in.S3Bucket,
		Key:          aws.String(key),
		Body:         r,
		ContentType:  aws.String("application/zip"),
		StorageClass: &in.S3StorageClass,
	}
	slog.InfoContext(ctx, "Uploading", "object", object, "s3-key", *upIn.Key)
	_, err := in.S3Uploader.UploadWithContext(ctx, upIn)
	if err != nil {
		return fmt.Errorf("upload to s3: %w", err)
	}
	slog.InfoContext(ctx, "Uploaded", "object", object, "s3-key", *upIn.Key)

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
				slog.InfoContext(ctx, "To delete", "s3-key", *obj.Key)
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
	if !in.DryRun {
		_, err = in.S3Service.DeleteObjectsWithContext(ctx, &s3.DeleteObjectsInput{
			Bucket: &in.S3Bucket,
			Delete: &s3.Delete{
				Objects: dels,
			},
		})
		if err != nil {
			return 0, fmt.Errorf("delete objects: %w", err)
		}
		slog.InfoContext(ctx, "Deleted objects", "len", len(dels))
	}
	return len(dels), nil
}

func makeS3Key(localPath, outPrefix, object string) string {
	return filepath.ToSlash(filepath.Join(outPrefix, filepath.Base(localPath), object)) + ".zip"
}
