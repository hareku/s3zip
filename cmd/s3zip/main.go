package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/hareku/s3zip"
	"golang.org/x/exp/slog"
)

func main() {
	if err := run(); err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	if len(os.Args) < 2 {
		return fmt.Errorf("usage: %s <config file>", os.Args[0])
	}
	conf, err := s3zip.ReadConfig(os.Args[1])
	if err != nil {
		return fmt.Errorf("read config[%s]: %w", os.Args[1], err)
	}
	slog.InfoContext(ctx, fmt.Sprintf("Loaded config: %+v", conf))

	s3svc := s3.New(session.Must(session.NewSession()), &aws.Config{
		Region: aws.String(conf.S3.Region),
	})
	uploader := s3manager.NewUploaderWithClient(s3svc)

	for _, t := range conf.Targets {
		slog.InfoContext(ctx, "Start", "target", t)
		out, err := s3zip.Run(ctx, &s3zip.RunInput{
			DryRun:         conf.DryRun,
			S3Bucket:       conf.S3.Bucket,
			S3StorageClass: conf.S3.StorageClass,
			S3Uploader:     uploader,
			S3Service:      s3svc,
			Path:           t.Path,
			ZipDepth:       t.ZipDepth,
			OutPrefix:      t.OutPrefix,
		})
		if err != nil {
			return fmt.Errorf("run %q: %w", t.Path, err)
		}
		log.Printf("Done %q: %v", t.Path, out)
	}
	return nil
}
