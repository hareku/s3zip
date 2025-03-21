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
	"github.com/hareku/s3zip"
)

var (
	configFlag      = flag.String("config", "", "config file path")
	dryFlag         = flag.Bool("dry", false, "dry run")
	debugFlag       = flag.Bool("debug", false, "debug mode")
	concurrencyFlag = flag.Int("concurrency", s3zip.DefaultConcurrency, "concurrency")
)

var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

func main() {
	flag.Parse()
	setupLogger()

	if err := run(); err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}
}

func setupLogger() {
	level := slog.LevelInfo
	if *debugFlag {
		level = slog.LevelDebug
	}

	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: level,
	})))
}

func run() error {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	slog.InfoContext(ctx, "s3zip", "version", version, "commit", commit, "date", date)

	if *configFlag == "" {
		return fmt.Errorf("config flag is required")
	}
	if *dryFlag {
		slog.InfoContext(ctx, "Dry run is enabled")
	}

	slog.InfoContext(ctx, "Loading config", "path", *configFlag)
	conf, err := s3zip.ReadConfig(*configFlag)
	if err != nil {
		return fmt.Errorf("read config: %w", err)
	}
	slog.InfoContext(ctx, "Loaded config", "targets_count", len(conf.Targets))

	s3svc := s3.New(session.Must(session.NewSession()), &aws.Config{
		Region: aws.String(conf.S3.Region),
	})

	for i, t := range conf.Targets {
		slog.InfoContext(ctx, "Start", "i", i, "target", t)
		result, err := s3zip.Run(ctx, &s3zip.RunInput{
			DryRun:           *dryFlag,
			S3Bucket:         conf.S3.Bucket,
			S3StorageClass:   conf.S3.StorageClass,
			S3Service:        s3svc,
			MetadataStoreKey: conf.Metadata,
			Path:             t.Path,
			MaxZipDepth:      t.MaxZipDepth,
			OutPrefix:        t.OutPrefix,
			Concurrency:      *concurrencyFlag,
		})
		if err != nil {
			return fmt.Errorf("run: %w", err)
		}
		slog.InfoContext(ctx, "Done", "result", result)
	}
	return nil
}
