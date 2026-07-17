package volumes

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// S3Config points the store at an S3/MinIO bucket. Prefix scopes all keys (so
// one bucket can hold several clusters' snapshots).
type S3Config struct {
	Endpoint  string // host:port, no scheme
	Region    string
	Bucket    string
	Prefix    string // optional key prefix, e.g. "zattera/"
	AccessKey string
	SecretKey string
	UseSSL    bool
}

// S3Store is an ObjectStore backed by S3/MinIO (minio-go).
type S3Store struct {
	cli    *minio.Client
	bucket string
	prefix string
}

// NewS3Store connects to the bucket. Objects are ≤ a few MB, so no multipart.
func NewS3Store(cfg S3Config) (*S3Store, error) {
	cli, err := minio.New(cfg.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, ""),
		Secure: cfg.UseSSL,
		Region: cfg.Region,
	})
	if err != nil {
		return nil, fmt.Errorf("volumes: s3 connect: %w", err)
	}
	prefix := cfg.Prefix
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	return &S3Store{cli: cli, bucket: cfg.Bucket, prefix: prefix}, nil
}

func (s *S3Store) key(k string) string { return s.prefix + k }

func (s *S3Store) Has(ctx context.Context, key string) (bool, error) {
	_, err := s.cli.StatObject(ctx, s.bucket, s.key(key), minio.StatObjectOptions{})
	if err == nil {
		return true, nil
	}
	if isNotFound(err) {
		return false, nil
	}
	return false, err
}

func (s *S3Store) Get(ctx context.Context, key string) ([]byte, error) {
	obj, err := s.cli.GetObject(ctx, s.bucket, s.key(key), minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	defer func() { _ = obj.Close() }()
	data, err := io.ReadAll(obj)
	if err != nil {
		if isNotFound(err) {
			return nil, errNotFound{key}
		}
		return nil, err
	}
	return data, nil
}

func (s *S3Store) Put(ctx context.Context, key string, data []byte) error {
	_, err := s.cli.PutObject(ctx, s.bucket, s.key(key), bytes.NewReader(data), int64(len(data)),
		minio.PutObjectOptions{ContentType: "application/octet-stream"})
	return err
}

func (s *S3Store) List(ctx context.Context, prefix string) ([]string, error) {
	var out []string
	for obj := range s.cli.ListObjects(ctx, s.bucket, minio.ListObjectsOptions{
		Prefix: s.key(prefix), Recursive: true,
	}) {
		if obj.Err != nil {
			return nil, obj.Err
		}
		out = append(out, strings.TrimPrefix(obj.Key, s.prefix))
	}
	return out, nil
}

func (s *S3Store) Delete(ctx context.Context, key string) error {
	return s.cli.RemoveObject(ctx, s.bucket, s.key(key), minio.RemoveObjectOptions{})
}

// isNotFound reports whether err is an S3 "no such key" (or a 404).
func isNotFound(err error) bool {
	if err == nil {
		return false
	}
	resp := minio.ToErrorResponse(err)
	if resp.Code == "NoSuchKey" || resp.StatusCode == http.StatusNotFound {
		return true
	}
	return errors.Is(err, io.EOF)
}
