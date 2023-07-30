# s3zip

s3zip zip files and upload to Amazon S3.

## Motivation

Ref: https://aws.amazon.com/s3/pricing/

- Reduce the metadata:
  - For each object that is stored in S3 Glacier, Amazon S3 adds 40 KB of chargeable overhead for metadata, with 8 KB charged at S3 Standard rates and 32 KB charged at S3 Glacier Flexible Retrieval or S3 Deep Archive rates.
- Reduce API requests:
  - Amazon S3 charges for API requests. e.g. PUT, COPY, POST, LIST requests (per 1,000 requests).

## Usage

```bash
s3zip path/to/config.yaml
```

## Config

```yaml
dry_run: true

s3:
  region: us-west-2
  bucket: my-bucket
  storage_class: DEEP_ARCHIVE # STANDARD | DEEP_ARCHIVE | etc.

targets:
  - path: D:\User\Desktop\MyPictures
    max_zip_depth: 2 # 0: zip MyPictures folder, 1: zip files under MyPictures/*, 2: zip files under MyPictures/**/*
    out_prefix: s3zip # prefix for s3 object key
```
