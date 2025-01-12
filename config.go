package s3zip

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

type Config struct {
	S3      ConfigS3       `yaml:"s3"`
	Targets []ConfigTarget `yaml:"targets"`
}

type ConfigS3 struct {
	Region       string `yaml:"region"`
	Bucket       string `yaml:"bucket"`
	StorageClass string `yaml:"storage_class"`
}

type ConfigTarget struct {
	Path        string `yaml:"path"`
	MaxZipDepth int    `yaml:"max_zip_depth"`
	OutPrefix   string `yaml:"out_prefix"`
}

func ReadConfig(name string) (*Config, error) {
	f, err := os.Open(name)
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
