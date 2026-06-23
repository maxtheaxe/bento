package aws

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/public/service"
)

func TestS3StreamOutputConfigParsing(t *testing.T) {
	tests := []struct {
		name        string
		config      string
		expectError bool
	}{
		{
			name: "basic config",
			config: `
bucket: test-bucket
path: 'logs/${! timestamp_unix() }.log'
`,
			expectError: false,
		},
		{
			name: "with partition_by",
			config: `
bucket: test-bucket
path: 'logs/date=${! meta("date") }/${! uuid_v4() }.log'
partition_by:
  - '${! meta("date") }'
  - '${! meta("account") }'
`,
			expectError: false,
		},
		{
			name: "with buffer settings",
			config: `
bucket: test-bucket
path: 'data/${! timestamp_unix() }.json'
max_buffer_bytes: 5242880
max_buffer_count: 5000
max_buffer_period: 5s
`,
			expectError: false,
		},
		{
			name: "with file rotation settings",
			config: `
bucket: test-bucket
path: 'data/${! timestamp_unix() }.json'
max_file_bytes: 104857600
max_file_count: 100000
max_file_period: 1h
`,
			expectError: false,
		},
		{
			name: "with content type",
			config: `
bucket: test-bucket
path: 'logs/${! timestamp_unix() }.log'
content_type: 'application/json'
`,
			expectError: false,
		},
		{
			name: "with content encoding",
			config: `
bucket: test-bucket
path: 'logs/${! timestamp_unix() }.log.gz'
content_type: 'application/json'
content_encoding: 'gzip'
`,
			expectError: false,
		},
		{
			name: "missing bucket",
			config: `
path: 'logs/${! timestamp_unix() }.log'
`,
			expectError: true,
		},
		{
			name: "missing path",
			config: `
bucket: test-bucket
`,
			expectError: true,
		},
		{
			name: "invalid period",
			config: `
bucket: test-bucket
path: 'logs/${! timestamp_unix() }.log'
max_buffer_period: 'invalid'
`,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			spec := s3StreamOutputSpec()
			parsedConf, err := spec.ParseYAML(tt.config, nil)

			if tt.expectError && err != nil {
				// Error during parsing is acceptable for error test cases
				return
			}

			require.NoError(t, err)

			conf, err := s3StreamConfigFromParsed(parsedConf)
			if tt.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.NotEmpty(t, conf.Bucket)
			}
		})
	}
}

func TestS3StreamOutputPathInterpolation(t *testing.T) {
	configYAML := `
bucket: test-bucket
path: 'logs/date=${! meta("date") }/service=${! meta("service") }/${! uuid_v4() }.log'
partition_by:
  - '${! meta("date") }'
  - '${! meta("service") }'
`

	spec := s3StreamOutputSpec()
	parsedConf, err := spec.ParseYAML(configYAML, nil)
	require.NoError(t, err)

	conf, err := s3StreamConfigFromParsed(parsedConf)
	require.NoError(t, err)

	assert.Equal(t, "test-bucket", conf.Bucket)
	assert.NotNil(t, conf.Path)
	assert.Len(t, conf.PartitionBy, 2)

	// Test path interpolation with batch
	batch := service.MessageBatch{
		service.NewMessage([]byte(`{"message": "test1"}`)),
		service.NewMessage([]byte(`{"message": "test2"}`)),
	}

	batch[0].MetaSet("date", "2026-01-20")
	batch[0].MetaSet("service", "api")
	batch[1].MetaSet("date", "2026-01-20")
	batch[1].MetaSet("service", "web")

	// Evaluate partition_by for first message
	part0, err := batch.TryInterpolatedString(0, conf.PartitionBy[0])
	require.NoError(t, err)
	assert.Equal(t, "2026-01-20", part0)

	part1, err := batch.TryInterpolatedString(0, conf.PartitionBy[1])
	require.NoError(t, err)
	assert.Equal(t, "api", part1)

	// Evaluate partition_by for second message
	part2, err := batch.TryInterpolatedString(1, conf.PartitionBy[0])
	require.NoError(t, err)
	assert.Equal(t, "2026-01-20", part2)

	part3, err := batch.TryInterpolatedString(1, conf.PartitionBy[1])
	require.NoError(t, err)
	assert.Equal(t, "web", part3)

	// Path should contain interpolated values
	path0, err := batch.TryInterpolatedString(0, conf.Path)
	require.NoError(t, err)
	assert.Contains(t, path0, "date=2026-01-20")
	assert.Contains(t, path0, "service=api")

	path1, err := batch.TryInterpolatedString(1, conf.Path)
	require.NoError(t, err)
	assert.Contains(t, path1, "date=2026-01-20")
	assert.Contains(t, path1, "service=web")
}

func TestS3StreamOutputPartitionGrouping(t *testing.T) {
	configYAML := `
bucket: test-bucket
path: 'logs/date=${! meta("date") }/${! uuid_v4() }.log'
partition_by:
  - '${! meta("date") }'
`

	spec := s3StreamOutputSpec()
	parsedConf, err := spec.ParseYAML(configYAML, nil)
	require.NoError(t, err)

	conf, err := s3StreamConfigFromParsed(parsedConf)
	require.NoError(t, err)

	// Create batch with 4 messages, 2 dates
	batch := service.MessageBatch{
		service.NewMessage([]byte(`{"msg": "1"}`)),
		service.NewMessage([]byte(`{"msg": "2"}`)),
		service.NewMessage([]byte(`{"msg": "3"}`)),
		service.NewMessage([]byte(`{"msg": "4"}`)),
	}

	batch[0].MetaSet("date", "2026-01-20")
	batch[1].MetaSet("date", "2026-01-21")
	batch[2].MetaSet("date", "2026-01-20")
	batch[3].MetaSet("date", "2026-01-21")

	// Simulate partition grouping logic
	type partitionGroup struct {
		key  string
		path string
		msgs service.MessageBatch
	}
	partitionMap := make(map[string]*partitionGroup)

	for i, msg := range batch {
		partitionParts := make([]string, len(conf.PartitionBy))
		for j, partExpr := range conf.PartitionBy {
			var err error
			partitionParts[j], err = batch.TryInterpolatedString(i, partExpr)
			require.NoError(t, err)
		}
		partitionKey := partitionParts[0]

		var fullPath string
		if pg, exists := partitionMap[partitionKey]; exists {
			fullPath = pg.path
		} else {
			fullPath, err = batch.TryInterpolatedString(i, conf.Path)
			require.NoError(t, err)
		}

		if pg, exists := partitionMap[partitionKey]; exists {
			pg.msgs = append(pg.msgs, msg)
		} else {
			partitionMap[partitionKey] = &partitionGroup{
				key:  partitionKey,
				path: fullPath,
				msgs: service.MessageBatch{msg},
			}
		}
	}

	// Verify partitioning
	assert.Len(t, partitionMap, 2, "Should have 2 partitions")

	pg1 := partitionMap["2026-01-20"]
	require.NotNil(t, pg1)
	assert.Len(t, pg1.msgs, 2, "Partition 2026-01-20 should have 2 messages")
	assert.Contains(t, pg1.path, "date=2026-01-20")

	pg2 := partitionMap["2026-01-21"]
	require.NotNil(t, pg2)
	assert.Len(t, pg2.msgs, 2, "Partition 2026-01-21 should have 2 messages")
	assert.Contains(t, pg2.path, "date=2026-01-21")

	// Verify paths are different (UUID should be different per partition)
	assert.NotEqual(t, pg1.path, pg2.path, "Paths should be different for different partitions")
}

func TestS3StreamOutputBufferSettings(t *testing.T) {
	tests := []struct {
		name              string
		config            string
		expectedBytes     int64
		expectedCount     int
		expectedPeriodStr string
	}{
		{
			name: "default values",
			config: `
bucket: test-bucket
path: 'logs/test.log'
`,
			expectedBytes:     10 * 1024 * 1024, // 10MB
			expectedCount:     10000,
			expectedPeriodStr: "10s",
		},
		{
			name: "custom values",
			config: `
bucket: test-bucket
path: 'logs/test.log'
max_buffer_bytes: 5242880
max_buffer_count: 5000
max_buffer_period: 5s
`,
			expectedBytes:     5242880,
			expectedCount:     5000,
			expectedPeriodStr: "5s",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			spec := s3StreamOutputSpec()
			parsedConf, err := spec.ParseYAML(tt.config, nil)
			require.NoError(t, err)

			conf, err := s3StreamConfigFromParsed(parsedConf)
			require.NoError(t, err)

			assert.Equal(t, tt.expectedBytes, conf.MaxBufferBytes)
			assert.Equal(t, tt.expectedCount, conf.MaxBufferCount)
			assert.Equal(t, tt.expectedPeriodStr, conf.MaxBufferPeriod.String())
		})
	}
}

func TestS3StreamOutputFileRotationSettings(t *testing.T) {
	tests := []struct {
		name           string
		config         string
		expectedBytes  int64
		expectedCount  int
		expectedPeriod string
	}{
		{
			name: "default values",
			config: `
bucket: test-bucket
path: 'logs/test.log'
`,
			expectedBytes:  0,
			expectedCount:  0,
			expectedPeriod: "0s",
		},
		{
			name: "custom values",
			config: `
bucket: test-bucket
path: 'logs/test.log'
max_file_bytes: 104857600
max_file_count: 50000
max_file_period: 1h
`,
			expectedBytes:  104857600,
			expectedCount:  50000,
			expectedPeriod: "1h0m0s",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			spec := s3StreamOutputSpec()
			parsedConf, err := spec.ParseYAML(tt.config, nil)
			require.NoError(t, err)

			conf, err := s3StreamConfigFromParsed(parsedConf)
			require.NoError(t, err)

			assert.Equal(t, tt.expectedBytes, conf.MaxFileBytes)
			assert.Equal(t, tt.expectedCount, conf.MaxFileCount)
			assert.Equal(t, tt.expectedPeriod, conf.MaxFilePeriod.String())
		})
	}
}

func TestS3StreamOutputContentSettings(t *testing.T) {
	configYAML := `
bucket: test-bucket
path: 'logs/test.log.gz'
content_type: 'application/json'
content_encoding: 'gzip'
`

	spec := s3StreamOutputSpec()
	parsedConf, err := spec.ParseYAML(configYAML, nil)
	require.NoError(t, err)

	conf, err := s3StreamConfigFromParsed(parsedConf)
	require.NoError(t, err)

	// Create test batch
	batch := service.MessageBatch{
		service.NewMessage([]byte(`{"test": "data"}`)),
	}

	// Evaluate content type
	contentType, err := batch.TryInterpolatedString(0, conf.ContentType)
	require.NoError(t, err)
	assert.Equal(t, "application/json", contentType)

	// Evaluate content encoding
	require.NotNil(t, conf.ContentEncoding)
	contentEncoding, err := batch.TryInterpolatedString(0, conf.ContentEncoding)
	require.NoError(t, err)
	assert.Equal(t, "gzip", contentEncoding)
}

func TestS3StreamOutputNoPartitionBy(t *testing.T) {
	// Test backwards compatibility when partition_by is not specified
	configYAML := `
bucket: test-bucket
path: 'logs/${! meta("filename") }.log'
`

	spec := s3StreamOutputSpec()
	parsedConf, err := spec.ParseYAML(configYAML, nil)
	require.NoError(t, err)

	conf, err := s3StreamConfigFromParsed(parsedConf)
	require.NoError(t, err)

	assert.Empty(t, conf.PartitionBy, "Should have no partition_by expressions")

	// When partition_by is empty, path should be evaluated per message
	batch := service.MessageBatch{
		service.NewMessage([]byte(`{"msg": "1"}`)),
		service.NewMessage([]byte(`{"msg": "2"}`)),
	}

	batch[0].MetaSet("filename", "file1")
	batch[1].MetaSet("filename", "file2")

	path0, err := batch.TryInterpolatedString(0, conf.Path)
	require.NoError(t, err)
	assert.Equal(t, "logs/file1.log", path0)

	path1, err := batch.TryInterpolatedString(1, conf.Path)
	require.NoError(t, err)
	assert.Equal(t, "logs/file2.log", path1)

	// Each message should create its own partition
	assert.NotEqual(t, path0, path1, "Paths should be different without partition_by")
}

func TestS3StreamOutputRotatesOnFileCount(t *testing.T) {
	var createCalls int
	var completeCalls int
	var completedUploadIDs []string
	mockClient := &mockS3StreamClient{
		createMultipartUploadFunc: func(ctx context.Context, input *s3.CreateMultipartUploadInput, opts ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error) {
			createCalls++
			return &s3.CreateMultipartUploadOutput{
				UploadId: aws.String(string(rune('a' + createCalls - 1))),
			}, nil
		},
		completeMultipartUploadFunc: func(ctx context.Context, input *s3.CompleteMultipartUploadInput, opts ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error) {
			completeCalls++
			completedUploadIDs = append(completedUploadIDs, *input.UploadId)
			return &s3.CompleteMultipartUploadOutput{}, nil
		},
	}

	path, err := service.NewInterpolatedString("logs/${! uuid_v4() }.log")
	require.NoError(t, err)
	partitionBy, err := service.NewInterpolatedString("all")
	require.NoError(t, err)
	contentType, err := service.NewInterpolatedString("application/octet-stream")
	require.NoError(t, err)

	output, err := newS3StreamOutput(s3StreamConfig{
		Bucket:          "test-bucket",
		Path:            path,
		PartitionBy:     []*service.InterpolatedString{partitionBy},
		MaxBufferBytes:  10 * 1024 * 1024,
		MaxBufferCount:  10000,
		MaxBufferPeriod: time.Hour,
		MaxFileCount:    2,
		ContentType:     contentType,
	}, service.MockResources())
	require.NoError(t, err)
	output.s3Client = mockClient

	batch := service.MessageBatch{
		service.NewMessage([]byte("one")),
		service.NewMessage([]byte("two")),
	}
	require.NoError(t, output.WriteBatch(context.Background(), batch))

	assert.Equal(t, 1, createCalls)
	assert.Equal(t, 1, completeCalls)
	assert.Equal(t, []string{"a"}, completedUploadIDs)
	assert.Empty(t, output.writers)

	require.NoError(t, output.WriteBatch(context.Background(), service.MessageBatch{
		service.NewMessage([]byte("three")),
	}))

	assert.Equal(t, 2, createCalls)
	assert.Equal(t, 1, completeCalls)
	assert.Len(t, output.writers, 1)
}
