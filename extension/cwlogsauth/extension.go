// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT

// Package cwlogsauth provides an OTel auth extension that intercepts outgoing
// HTTP requests to the CloudWatch Logs OTLP endpoint, dynamically sets the
// x-aws-log-group header based on service.name, and lazily creates log groups
// and streams on first encounter.
//
// It implements extensionauth.HTTPClient so it participates in the auth chain:
//
//	cwlogsauth → sigv4auth → network
//
// The extension reads service.name from client.Metadata (populated by the
// clientmetadata processor), resolves the log group name template (e.g.
// /test/telemetry/{ServiceName}), sets the x-aws-log-group header, creates
// the log group/stream if needed, then delegates to the inner auth (sigv4auth)
// for request signing.
//
// Auth chaining: During Start(), cwlogsauth resolves the inner auth extension
// (e.g. sigv4auth) from the host. In RoundTripper(), it chains:
//
//	cwlogsauth.RoundTrip → sigv4auth.RoundTrip → base HTTP transport
//
// Design considerations:
//
//   - First request per (group, stream): Blocks synchronously while creating the
//     log group/stream. Includes jitter (0-500ms) to spread thundering-herd load.
//
//   - Subsequent requests: Hit the positive cache (sync.Map) with zero overhead.
//
//   - Negative cache: Failed creations are cached with a TTL (default 30s).
//
//   - Singleflight dedup: Concurrent requests for the same (group, stream) wait
//     on a channel rather than launching duplicate API calls.
//
//   - Never returns errors to the pipeline: Even if creation fails, the request
//     proceeds. The CW endpoint rejects it, and the exporter retries.
package cwlogsauth

import (
	"context"
	"fmt"
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
	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/extension/extensionauth"
	"go.opentelemetry.io/collector/extension/extensioncapabilities"
	"go.uber.org/zap"
)

var cwLogsEndpointPattern = regexp.MustCompile(`^https://logs\.([a-z0-9-]+)\.amazonaws\.com`)

// Verify interface compliance.
var (
	_ component.Component             = (*cwLogsAuthExtension)(nil)
	_ extensionauth.HTTPClient        = (*cwLogsAuthExtension)(nil)
	_ extensioncapabilities.Dependent = (*cwLogsAuthExtension)(nil)
)

type provisionStatus struct {
	success   bool
	timestamp time.Time
}

type inflightEntry struct {
	done chan struct{}
}

type cwLogsAuthExtension struct {
	logger *zap.Logger
	cfg    *Config

	// innerAuth is the inner auth extension (e.g., sigv4auth) resolved during Start().
	innerAuth extensionauth.HTTPClient

	provisioned    sync.Map
	inflight       sync.Map
	failureBackoff time.Duration
}

func newExtension(logger *zap.Logger, cfg *Config) *cwLogsAuthExtension {
	backoff := 30 * time.Second
	if cfg.FailureBackoffSeconds > 0 {
		backoff = time.Duration(cfg.FailureBackoffSeconds) * time.Second
	}
	return &cwLogsAuthExtension{
		logger:         logger,
		cfg:            cfg,
		failureBackoff: backoff,
	}
}

// Start resolves the inner auth extension from the host.
func (e *cwLogsAuthExtension) Start(_ context.Context, host component.Host) error {
	if e.cfg.Auth == nil {
		e.logger.Info("cwlogsauth started without inner auth — requests will not be signed")
		return nil
	}

	// Look up the inner auth extension
	ext, ok := host.GetExtensions()[*e.cfg.Auth]
	if !ok {
		return fmt.Errorf("inner auth extension %q not found", e.cfg.Auth)
	}

	httpClient, ok := ext.(extensionauth.HTTPClient)
	if !ok {
		return fmt.Errorf("inner auth extension %q does not implement HTTPClient", e.cfg.Auth)
	}

	e.innerAuth = httpClient
	e.logger.Info("cwlogsauth started with inner auth", zap.String("innerAuth", e.cfg.Auth.String()))
	return nil
}

func (e *cwLogsAuthExtension) Shutdown(_ context.Context) error {
	return nil
}

func (e *cwLogsAuthExtension) Dependencies() []component.ID {
	if e.cfg.Auth != nil {
		return []component.ID{*e.cfg.Auth}
	}
	return nil
}

// RoundTripper implements extensionauth.HTTPClient.
// It chains: cwlogsauth → innerAuth (sigv4auth) → base transport.
func (e *cwLogsAuthExtension) RoundTripper(base http.RoundTripper) (http.RoundTripper, error) {
	// Chain the inner auth first (sigv4auth wraps the base transport)
	transport := base
	if e.innerAuth != nil {
		var err error
		transport, err = e.innerAuth.RoundTripper(base)
		if err != nil {
			return nil, fmt.Errorf("failed to get inner auth RoundTripper: %w", err)
		}
	}

	return &cwLogsRoundTripper{
		base: transport, // sigv4auth's RoundTripper
		ext:  e,
	}, nil
}

type cwLogsRoundTripper struct {
	base http.RoundTripper
	ext  *cwLogsAuthExtension
}

func (rt *cwLogsRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	// Resolve the dynamic log group name from client.Metadata
	logGroup, logStream := rt.ext.resolveHeaders(req)

	if logGroup != "" {
		// Clone request and set headers before signing
		req2 := req.Clone(req.Context())
		req2.Header.Set("x-aws-log-group", logGroup)
		req2.Header.Set("x-aws-log-stream", logStream)

		// Ensure log group/stream exists (blocks on first encounter, cached after)
		rt.ext.ensureProvisioned(req2, logGroup, logStream)

		// Pass to inner auth (sigv4auth) which will sign the request with these headers
		return rt.base.RoundTrip(req2)
	}

	return rt.base.RoundTrip(req)
}

func (e *cwLogsAuthExtension) resolveHeaders(req *http.Request) (logGroup, logStream string) {
	cl := client.FromContext(req.Context())
	serviceNames := cl.Metadata.Get("service.name")

	serviceName := e.cfg.DefaultServiceName
	if len(serviceNames) > 0 && serviceNames[0] != "" {
		serviceName = serviceNames[0]
	}

	logGroup = strings.ReplaceAll(e.cfg.LogGroupName, "{ServiceName}", serviceName)
	logStream = e.cfg.LogStreamName
	if logStream == "" {
		logStream = "default"
	}

	return logGroup, logStream
}

func (e *cwLogsAuthExtension) ensureProvisioned(req *http.Request, logGroup, logStream string) {
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
		region = extractRegionFromURL(req.URL.String())
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
