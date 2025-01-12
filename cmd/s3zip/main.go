package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"

	"log/slog"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/hareku/s3zip"
)

var (
	configFlag = flag.String("config", "", "config file path")
	dryFlag    = flag.Bool("dry", false, "dry run")
	debugFlag  = flag.Bool("debug", false, "debug mode")
)

var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
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

	flag.Parse()
	{
		logLevel := slog.LevelInfo
		if *debugFlag {
			logLevel = slog.LevelDebug
		}
		slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
			Level: logLevel,
		})))
	}
	slog.InfoContext(ctx, "s3zip", "version", version, "commit", commit, "date", date)

	if *configFlag == "" {
		return fmt.Errorf("config flag is required")
	}
	if *dryFlag {
		slog.InfoContext(ctx, "Dry run is enabled")
	}

	slog.InfoContext(ctx, "Reading config", "config", *configFlag)
	conf, err := s3zip.ReadConfig(*configFlag)
	if err != nil {
		return fmt.Errorf("read config: %w", err)
	}
	slog.DebugContext(ctx, fmt.Sprintf("Loaded config: %+v", conf))

	s3svc := s3.New(session.Must(session.NewSession()), &aws.Config{
		Region: aws.String(conf.S3.Region),
	})
	uploader := s3manager.NewUploaderWithClient(s3svc, func(u *s3manager.Uploader) {
		u.PartSize = 64 * 1024 * 1024
	})

	for _, t := range conf.Targets {
		slog.InfoContext(ctx, "Start", "target", t)
		out, err := s3zip.Run(ctx, &s3zip.RunInput{
			DryRun:         *dryFlag,
			S3Bucket:       conf.S3.Bucket,
			S3StorageClass: conf.S3.StorageClass,
			S3Uploader:     uploader,
			S3Service:      s3svc,
			Path:           t.Path,
			MaxZipDepth:    t.MaxZipDepth,
			OutPrefix:      t.OutPrefix,
		})
		if err != nil {
			return fmt.Errorf("run %q: %w", t.Path, err)
		}
		slog.InfoContext(ctx, "Done", "target", t, "output", out)
	}
	return nil
}
