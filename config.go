package s3zip

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Targets []ConfigTarget
}

type ConfigS3 struct {
	Bucket string
}

type ConfigTarget struct {
	Path      string
	ZipDepth  int
	OutPrefix string
}

func ReadConfig(path string) (*Config, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open config file: %w", err)
	}
	defer f.Close()

	var c Config
	if err := yaml.NewDecoder(f).Decode(&c); err != nil {
		return nil, fmt.Errorf("decode config file: %w", err)
	}
	return &c, nil
}
