package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/hareku/s3zip"
)

func main() {
	if err := run(); err != nil {
		log.Fatalf("error: %s", err)
	}
	log.Println("done")
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	if len(os.Args) < 2 {
		return fmt.Errorf("usage: %s <config file>", os.Args[0])
	}
	conf, err := s3zip.ReadConfig(os.Args[1])
	if err != nil {
		return fmt.Errorf("read config[%s]: %s", os.Args[1], err)
	}
}
