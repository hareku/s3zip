package s3zip

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"log/slog"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/dustin/go-humanize"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"
)

const (
	DefaultMetadataStoreKey = "s3zip-metadata.pb"
	DefaultConcurrency      = 10
)

type (
	RunInput struct {
		DryRun           bool
		S3Bucket         string
		S3Service        *s3.S3
		MetadataStoreKey string
		Path             string
		MaxZipDepth      int
		OutPrefix        string
		S3StorageClass   string
	}

	RunOutput struct {
		Upload int
		Delete int
	}

	ObjectToUpload struct {
		Name string
		Hash string
		Size int
	}

	runClient struct {
		dryRun bool

		s3Bucket       string
		s3StorageClass string

		s3Service  *s3.S3
		s3Uploader *s3manager.Uploader

		metadataStoreKey string
		metadataStore    *MetadataStore
		mu               sync.Mutex

		path        string
		maxZipDepth int
		outPrefix   string

		concurrency int
	}
)

func newRunClient(in *RunInput) *runClient {
	c := runClient{
		dryRun: in.DryRun,

		s3Bucket:       in.S3Bucket,
		s3StorageClass: in.S3StorageClass,

		s3Service: in.S3Service,
		s3Uploader: s3manager.NewUploaderWithClient(in.S3Service, func(u *s3manager.Uploader) {
			u.PartSize = 64 * 1024 * 1024
		}),

		metadataStoreKey: in.MetadataStoreKey,

		path:        in.Path,
		maxZipDepth: in.MaxZipDepth,
		outPrefix:   in.OutPrefix,

		concurrency: DefaultConcurrency,
	}

	if c.metadataStoreKey == "" {
		c.metadataStoreKey = DefaultMetadataStoreKey
	}

	return &c
}

func Run(ctx context.Context, in *RunInput) (*RunOutput, error) {
	return newRunClient(in).run(ctx)
}

func (c *runClient) run(ctx context.Context) (*RunOutput, error) {
	objects, err := LocalObjects(c.path, c.maxZipDepth)
	if err != nil {
		return nil, fmt.Errorf("list local objects: %w", err)
	}
	slog.InfoContext(ctx, "Listed objects", "len", len(objects))

	if err := c.loadMetadataStore(ctx); err != nil {
		return nil, fmt.Errorf("load metadata store: %w", err)
	}
	defer func() {
		if c.dryRun {
			return
		}

		timeout := 30 * time.Second
		slog.DebugContext(ctx, "Saving metadata store", "timeout", timeout)

		ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), timeout)
		defer cancel()
		if err := c.saveMetadataStore(ctx, c.metadataStore); err != nil {
			slog.ErrorContext(ctx, "save metadata store", "error", err)
		}
		slog.InfoContext(ctx, "Saved metadata store")
	}()

	objectsToUpload, err := c.listObjectsToUpload(ctx, objects)
	if err != nil {
		return nil, fmt.Errorf("list objects to upload: %w", err)
	}
	slog.InfoContext(ctx, "Listed objects to upload", "len", len(objectsToUpload))

	if err := c.uploadObjects(ctx, objectsToUpload); err != nil {
		return nil, fmt.Errorf("upload objects: %w", err)
	}

	deletedLen, err := c.cleanUnusedObjects(ctx, objects)
	if err != nil {
		return nil, fmt.Errorf("clean unused objects: %w", err)
	}
	return &RunOutput{
		Upload: len(objectsToUpload),
		Delete: deletedLen,
	}, nil
}

func (c *runClient) listObjectsToUpload(ctx context.Context, objects []string) ([]ObjectToUpload, error) {
	res := make([]ObjectToUpload, 0, len(objects))

	eg, ctx := errgroup.WithContext(ctx)
	eg.SetLimit(c.concurrency)

	for _, object := range objects {
		eg.Go(func() error {
			if ctx.Err() != nil {
				return ctx.Err()
			}

			objectHash, err := Hash(filepath.Join(c.path, object))
			if err != nil {
				return fmt.Errorf("compute hash %q: %w", object, err)
			}
			key := makeS3Key(c.path, c.outPrefix, object)

			c.mu.Lock()
			defer c.mu.Unlock()

			if m, ok := c.metadataStore.Metadata[key]; ok && m.Hash == objectHash {
				return nil
			}

			size, err := Size(filepath.Join(c.path, object))
			if err != nil {
				return fmt.Errorf("compute size %q: %w", object, err)
			}

			res = append(res, ObjectToUpload{
				Name: object,
				Hash: objectHash,
				Size: size,
			})
			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		return nil, err
	}
	return res, nil
}

func (c *runClient) loadMetadataStore(ctx context.Context) error {
	slog.DebugContext(ctx, "Loading metadata store", "key", c.metadataStoreKey)

	out, err := c.s3Service.GetObjectWithContext(ctx, &s3.GetObjectInput{
		Bucket: &c.s3Bucket,
		Key:    aws.String(c.metadataStoreKey),
	})
	if err != nil {
		var aerr awserr.Error
		if errors.As(err, &aerr) && aerr.Code() == s3.ErrCodeNoSuchKey {
			slog.InfoContext(ctx, "Metadata store not found, creating a new one")
			c.metadataStore = &MetadataStore{
				Metadata: make(map[string]*Metadata),
			}
			return nil
		}

		return fmt.Errorf("get metadata from s3: %w", err)
	}

	b, err := io.ReadAll(out.Body)
	if err != nil {
		return fmt.Errorf("read body: %w", err)
	}

	var s MetadataStore
	if err := proto.Unmarshal(b, &s); err != nil {
		return fmt.Errorf("unmarshal: %w", err)
	}

	c.metadataStore = &s
	slog.InfoContext(ctx, "Loaded metadata store", "len", len(c.metadataStore.Metadata))

	return nil
}

func (c *runClient) saveMetadataStore(ctx context.Context, s *MetadataStore) error {
	b, err := proto.Marshal(s)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}

	_, err = c.s3Uploader.UploadWithContext(ctx, &s3manager.UploadInput{
		Bucket:       &c.s3Bucket,
		Key:          aws.String(c.metadataStoreKey),
		Body:         aws.ReadSeekCloser(io.NopCloser(bytes.NewReader(b))),
		ContentType:  aws.String("application/protobuf"),
		StorageClass: aws.String(s3.StorageClassStandard),
	})
	if err != nil {
		return fmt.Errorf("put object: %w", err)
	}
	return nil
}

func (c *runClient) uploadObjects(ctx context.Context, objects []ObjectToUpload) error {
	eg, ctx := errgroup.WithContext(ctx)
	eg.SetLimit(c.concurrency)

	for _, v := range objects {
		eg.Go(func() error {
			if err := c.uploadObject(ctx, v); err != nil {
				return fmt.Errorf("upload %q: %w", v.Name, err)
			}

			c.mu.Lock()
			c.metadataStore.Metadata[makeS3Key(c.path, c.outPrefix, v.Name)] = &Metadata{
				Hash: v.Hash,
			}
			c.mu.Unlock()

			return nil
		})
	}

	return eg.Wait()
}

func (c *runClient) uploadObject(ctx context.Context, v ObjectToUpload) error {
	slog.InfoContext(ctx, "Uploading", "name", v.Name, "size", humanize.Bytes(uint64(v.Size)))
	if c.dryRun {
		return nil
	}

	r := Zip(filepath.Join(c.path, v.Name))
	defer r.Close()

	in := &s3manager.UploadInput{
		Bucket:       &c.s3Bucket,
		Key:          aws.String(makeS3Key(c.path, c.outPrefix, v.Name)),
		Body:         r,
		ContentType:  aws.String("application/zip"),
		StorageClass: &c.s3StorageClass,
	}
	if _, err := c.s3Uploader.UploadWithContext(ctx, in); err != nil {
		return fmt.Errorf("upload to s3: %w", err)
	}
	return nil
}

func (c *runClient) cleanUnusedObjects(ctx context.Context, objects []string) (int, error) {
	mp := make(map[string]struct{})
	for _, v := range objects {
		mp[makeS3Key(c.path, c.outPrefix, v)] = struct{}{}
	}

	dels := make([]*s3.ObjectIdentifier, 0)
	err := c.s3Service.ListObjectsV2PagesWithContext(ctx, &s3.ListObjectsV2Input{
		Bucket: &c.s3Bucket,
		Prefix: aws.String(strings.TrimSuffix(c.outPrefix, "/") + "/"),
	}, func(page *s3.ListObjectsV2Output, lastPage bool) bool {
		for _, obj := range page.Contents {
			if _, ok := mp[*obj.Key]; !ok {
				slog.InfoContext(ctx, "Deleting", "s3-key", *obj.Key)
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
	if !c.dryRun {
		_, err = c.s3Service.DeleteObjectsWithContext(ctx, &s3.DeleteObjectsInput{
			Bucket: &c.s3Bucket,
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
