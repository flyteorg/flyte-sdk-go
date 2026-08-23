// Package storage provides blob-store IO by full URI (s3://…/inputs.pb etc.)
// for the task runtime, mirroring the Rust SDK's Storage: one lazily-built
// client cached per scheme://authority, a 10 MiB IO cap, and an
// AWS_ENDPOINT_URL override for minio/devbox.
package storage

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	s3v2 "github.com/aws/aws-sdk-go-v2/service/s3"
	"gocloud.dev/blob"
	"gocloud.dev/blob/gcsblob"
	"gocloud.dev/blob/s3blob"
	"gocloud.dev/gcp"

	_ "gocloud.dev/blob/azureblob" // azblob:// URL opener for az/abfs/abfss
)

// MaxIOBytes caps every read and write, matching the Python SDK's
// MAX_INLINE_IO_BYTES.
const MaxIOBytes = 10 * 1024 * 1024

// Storage is object storage with one lazily-built client cached per
// scheme://authority. Sharing a *Storage shares that cache, which is what lets
// a reusable container build its S3/GCS/Azure clients once and hand the same
// ones to every action it runs — credential resolution is not cheap.
type Storage struct {
	mu      sync.Mutex
	buckets map[string]*blob.Bucket
}

// New returns an empty Storage.
func New() *Storage {
	return &Storage{buckets: map[string]*blob.Bucket{}}
}

// Join joins a path segment onto a URI prefix.
func Join(base, name string) string {
	return strings.TrimRight(base, "/") + "/" + name
}

// Put writes data to the object at uri.
func (s *Storage) Put(ctx context.Context, uri string, data []byte) error {
	if len(data) > MaxIOBytes {
		return fmt.Errorf("payload for %s is %d bytes, exceeds the %d byte cap", uri, len(data), MaxIOBytes)
	}
	if path, ok := localPath(uri); ok {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return fmt.Errorf("failed to write %s: %w", uri, err)
		}
		if err := os.WriteFile(path, data, 0o644); err != nil {
			return fmt.Errorf("failed to write %s: %w", uri, err)
		}
		return nil
	}
	bucket, key, err := s.resolve(ctx, uri)
	if err != nil {
		return err
	}
	if err := bucket.WriteAll(ctx, key, data, nil); err != nil {
		return fmt.Errorf("failed to write %s: %w", uri, err)
	}
	return nil
}

// Get reads the object at uri.
func (s *Storage) Get(ctx context.Context, uri string) ([]byte, error) {
	if path, ok := localPath(uri); ok {
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("failed to read %s: %w", uri, err)
		}
		if len(data) > MaxIOBytes {
			return nil, fmt.Errorf("object at %s exceeds the %d byte cap", uri, MaxIOBytes)
		}
		return data, nil
	}
	bucket, key, err := s.resolve(ctx, uri)
	if err != nil {
		return nil, err
	}
	r, err := bucket.NewReader(ctx, key, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to open %s: %w", uri, err)
	}
	defer r.Close()
	data, err := io.ReadAll(io.LimitReader(r, MaxIOBytes+1))
	if err != nil {
		return nil, fmt.Errorf("failed to read %s: %w", uri, err)
	}
	if len(data) > MaxIOBytes {
		return nil, fmt.Errorf("object at %s exceeds the %d byte cap", uri, MaxIOBytes)
	}
	return data, nil
}

// localPath maps bare and file:// URIs to a filesystem path.
func localPath(uri string) (string, bool) {
	if strings.HasPrefix(uri, "file://") {
		return strings.TrimPrefix(uri, "file://"), true
	}
	if strings.HasPrefix(uri, "/") {
		return uri, true
	}
	return "", false
}

// resolve maps a cloud URI to a cached bucket plus the object key within it.
func (s *Storage) resolve(ctx context.Context, uri string) (*blob.Bucket, string, error) {
	u, err := url.Parse(uri)
	if err != nil {
		return nil, "", fmt.Errorf("invalid storage uri %s: %w", uri, err)
	}
	scheme, authority := u.Scheme, u.Host
	key := strings.TrimPrefix(u.Path, "/")
	cacheKey := scheme + "://" + authority

	bucket, err := s.cached(cacheKey, func() (*blob.Bucket, error) {
		switch scheme {
		case "s3", "s3a":
			return openS3(ctx, authority)
		case "gs":
			return openGCS(ctx, authority)
		case "az", "abfs", "abfss":
			// Delegate to the azblob URL opener (AZURE_STORAGE_ACCOUNT etc.).
			return blob.OpenBucket(ctx, "azblob://"+authority)
		default:
			return nil, fmt.Errorf("unsupported storage scheme %q in %s", scheme, uri)
		}
	})
	if err != nil {
		return nil, "", err
	}
	return bucket, key, nil
}

// openS3 builds an S3 bucket from ambient AWS config. AWS_ENDPOINT_URL (the
// obstore-style override) points it at minio/devbox; path-style addressing is
// forced there because virtual-host addressing breaks against
// http://minio:9000-style endpoints.
func openS3(ctx context.Context, bucketName string) (*blob.Bucket, error) {
	cfg, err := awsconfig.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}
	endpoint := os.Getenv("AWS_ENDPOINT_URL")
	client := s3v2.NewFromConfig(cfg, func(o *s3v2.Options) {
		if endpoint != "" {
			o.BaseEndpoint = aws.String(endpoint)
			o.UsePathStyle = true
		}
	})
	return s3blob.OpenBucketV2(ctx, client, bucketName, nil)
}

func openGCS(ctx context.Context, bucketName string) (*blob.Bucket, error) {
	creds, err := gcp.DefaultCredentials(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to load GCP credentials: %w", err)
	}
	client, err := gcp.NewHTTPClient(gcp.DefaultTransport(), gcp.CredentialsTokenSource(creds))
	if err != nil {
		return nil, fmt.Errorf("failed to build GCS client: %w", err)
	}
	return gcsblob.OpenBucket(ctx, client, bucketName, nil)
}

func (s *Storage) cached(key string, build func() (*blob.Bucket, error)) (*blob.Bucket, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if b, ok := s.buckets[key]; ok {
		return b, nil
	}
	b, err := build()
	if err != nil {
		return nil, err
	}
	s.buckets[key] = b
	return b, nil
}
