// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT

// Package cwotlplogs provides a CloudWatch Logs OTLP exporter that sends logs
// to the CW OTLP endpoint with dynamic per-service log group routing and
// automatic log group/stream creation.
//
// Unlike the generic otlphttp exporter, this exporter:
//   - Has full access to plog.Logs (resource attributes) to determine service.name
//   - Splits batches internally by service name — no batch processor metadata_keys needed
//   - Resolves {ServiceName} template in log group names
//   - Creates log groups/streams lazily on first encounter
//   - Sets x-aws-log-group/x-aws-log-stream headers per sub-batch
//
// Design considerations for fleet-wide restarts, retry storms, and latency:
//
//   - First request per (group, stream): Blocks synchronously while creating.
//     Includes jitter (0-500ms) to spread thundering-herd load.
//   - Subsequent requests: Hit the positive cache with zero overhead.
//   - Negative cache with TTL prevents retry storms.
//   - Singleflight dedup for concurrent creation of the same log group.
//   - Uses confighttp.ClientConfig for HTTP client setup (TLS, proxy, etc.)
//   - Uses sigv4auth for request signing (standard OTel auth mechanism).
package cwotlplogs

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/cloudwatchlogs"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.uber.org/zap"
)

var cwLogsEndpointPattern = regexp.MustCompile(`^https://logs\.([a-z0-9-]+)\.amazonaws\.com`)

type provisionStatus struct {
	success   bool
	timestamp time.Time
}

type inflightEntry struct {
	done chan struct{}
}

type cwOTLPLogsExporter struct {
	logger *zap.Logger
	cfg    *Config
	client *http.Client

	provisioned    sync.Map
	inflight       sync.Map
	failureBackoff time.Duration
}

func newExporter(settings exporter.Settings, cfg *Config) (*cwOTLPLogsExporter, error) {
	backoff := 30 * time.Second
	if cfg.FailureBackoffSeconds > 0 {
		backoff = time.Duration(cfg.FailureBackoffSeconds) * time.Second
	}
	return &cwOTLPLogsExporter{
		logger:         settings.Logger,
		cfg:            cfg,
		failureBackoff: backoff,
	}, nil
}

func (e *cwOTLPLogsExporter) start(ctx context.Context, host component.Host) error {
	client, err := e.cfg.ClientConfig.ToClient(ctx, host, component.TelemetrySettings{})
	if err != nil {
		return fmt.Errorf("failed to create HTTP client: %w", err)
	}
	e.client = client
	return nil
}

func (e *cwOTLPLogsExporter) shutdown(_ context.Context) error {
	return nil
}

// pushLogs receives plog.Logs, groups by service.name, and sends each group
// as a separate OTLP HTTP request with the correct x-aws-log-group header.
func (e *cwOTLPLogsExporter) pushLogs(ctx context.Context, ld plog.Logs) error {
	// Group ResourceLogs by resolved log group name
	groups := e.groupByService(ld)

	var errs []error
	for logGroup, groupLogs := range groups {
		logStream := e.cfg.LogStreamName
		if logStream == "" {
			logStream = "default"
		}

		// Ensure log group/stream exists
		if e.cfg.AutoCreate {
			e.ensureProvisioned(logGroup, logStream)
		}

		// Send this sub-batch
		if err := e.sendLogs(ctx, groupLogs, logGroup, logStream); err != nil {
			errs = append(errs, fmt.Errorf("log group %q: %w", logGroup, err))
		}
	}

	if len(errs) > 0 {
		// Return permanent error so the exporter doesn't retry individual sub-batch failures
		// as a single batch. Each sub-batch failure is independent.
		return consumererror.NewPermanent(fmt.Errorf("failed to send to %d log groups: %v", len(errs), errs))
	}
	return nil
}

// groupByService groups ResourceLogs by the resolved log group name.
func (e *cwOTLPLogsExporter) groupByService(ld plog.Logs) map[string]plog.Logs {
	groups := make(map[string]plog.Logs)

	for i := 0; i < ld.ResourceLogs().Len(); i++ {
		rl := ld.ResourceLogs().At(i)
		serviceName := e.cfg.DefaultServiceName

		if val, exists := rl.Resource().Attributes().Get("service.name"); exists {
			if s := val.AsString(); s != "" {
				serviceName = s
			}
		}

		logGroup := strings.ReplaceAll(e.cfg.LogGroupName, "{ServiceName}", serviceName)

		groupLogs, exists := groups[logGroup]
		if !exists {
			groupLogs = plog.NewLogs()
			groups[logGroup] = groupLogs
		}

		// Copy this ResourceLogs into the group
		rl.CopyTo(groupLogs.ResourceLogs().AppendEmpty())
	}

	return groups
}

// sendLogs serializes logs to OTLP protobuf and sends to the CW OTLP endpoint.
func (e *cwOTLPLogsExporter) sendLogs(ctx context.Context, ld plog.Logs, logGroup, logStream string) error {
	tr := plogotlp.NewExportRequestFromLogs(ld)
	body, err := tr.MarshalProto()
	if err != nil {
		return fmt.Errorf("failed to marshal OTLP logs: %w", err)
	}

	endpoint := e.cfg.ClientConfig.Endpoint
	url := strings.TrimRight(endpoint, "/") + "/v1/logs"

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("x-aws-log-group", logGroup)
	req.Header.Set("x-aws-log-stream", logStream)

	resp, err := e.client.Do(req)
	if err != nil {
		return fmt.Errorf("HTTP request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}

	respBody, _ := io.ReadAll(resp.Body)
	return fmt.Errorf("CW OTLP endpoint returned %d: %s", resp.StatusCode, string(respBody))
}

// ensureProvisioned creates the log group and stream if not already cached.
// Same pattern as POC 1: synchronous on first request, singleflight dedup,
// jitter for thundering herd, negative cache for failures.
func (e *cwOTLPLogsExporter) ensureProvisioned(logGroup, logStream string) {
	key := logGroup + "\x00" + logStream

	if val, ok := e.provisioned.Load(key); ok {
		status := val.(*provisionStatus)
		if status.success {
			return
		}
		if time.Since(status.timestamp) < e.failureBackoff {
			return
		}
	}

	entry := &inflightEntry{done: make(chan struct{})}
	if existing, loaded := e.inflight.LoadOrStore(key, entry); loaded {
		existingEntry := existing.(*inflightEntry)
		<-existingEntry.done
		return
	}

	defer func() {
		close(entry.done)
		e.inflight.Delete(key)
	}()

	region := e.cfg.Region
	if region == "" {
		region = extractRegionFromURL(e.cfg.ClientConfig.Endpoint)
	}
	if region == "" {
		e.logger.Warn("Cannot determine region for log group creation",
			zap.String("logGroup", logGroup),
		)
		return
	}

	// Jitter for thundering-herd mitigation
	jitter := time.Duration(rand.Int63n(int64(500 * time.Millisecond))) // nolint:gosec
	time.Sleep(jitter)

	e.logger.Info("Creating log group/stream",
		zap.String("logGroup", logGroup),
		zap.String("logStream", logStream),
		zap.String("region", region),
	)

	err := createLogGroupAndStream(region, logGroup, logStream)
	if err != nil {
		e.provisioned.Store(key, &provisionStatus{
			success:   false,
			timestamp: time.Now(),
		})
		e.logger.Warn("Failed to create log group/stream (request proceeds, will retry after backoff)",
			zap.String("logGroup", logGroup),
			zap.String("logStream", logStream),
			zap.Duration("backoff", e.failureBackoff),
			zap.Error(err),
		)
		return
	}

	e.provisioned.Store(key, &provisionStatus{
		success:   true,
		timestamp: time.Now(),
	})
	e.logger.Info("Successfully created log group/stream",
		zap.String("logGroup", logGroup),
		zap.String("logStream", logStream),
	)
}

func extractRegionFromURL(url string) string {
	matches := cwLogsEndpointPattern.FindStringSubmatch(url)
	if len(matches) < 2 {
		return ""
	}
	return matches[1]
}

func createLogGroupAndStream(region, logGroupName, logStreamName string) error {
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(region),
	})
	if err != nil {
		return fmt.Errorf("failed to create AWS session: %w", err)
	}

	svc := cloudwatchlogs.New(sess)

	_, err = svc.CreateLogGroup(&cloudwatchlogs.CreateLogGroupInput{
		LogGroupName: aws.String(logGroupName),
	})
	if err != nil && !isAlreadyExists(err) {
		return fmt.Errorf("CreateLogGroup %q: %w", logGroupName, err)
	}

	_, err = svc.CreateLogStream(&cloudwatchlogs.CreateLogStreamInput{
		LogGroupName:  aws.String(logGroupName),
		LogStreamName: aws.String(logStreamName),
	})
	if err != nil && !isAlreadyExists(err) {
		return fmt.Errorf("CreateLogStream %q in %q: %w", logStreamName, logGroupName, err)
	}

	return nil
}

func isAlreadyExists(err error) bool {
	if awsErr, ok := err.(awserr.Error); ok {
		return awsErr.Code() == cloudwatchlogs.ErrCodeResourceAlreadyExistsException
	}
	return false
}
