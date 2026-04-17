// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT

// Package cwotlplogs provides a CloudWatch Logs OTLP exporter that wraps the
// standard otlphttp exporter to add dynamic per-service log group routing and
// automatic log group/stream creation.
//
// Architecture:
//
//	cwotlplogs.pushLogs(ctx, plog.Logs)
//	  1. Groups ResourceLogs by service.name
//	  2. For each service group:
//	     a. Resolves {ServiceName} template -> log group name
//	     b. Creates log group/stream if needed (lazy, cached, jitter)
//	     c. Sets log_group/log_stream on client.Metadata in context
//	     d. Calls inner otlphttp exporter's ConsumeLogs(ctx, groupLogs)
//	        -> cwlogsheaders RoundTripper reads client.Metadata, sets x-aws-log-group
//	        -> sigv4auth signs the request (including the header)
//	        -> network
//
// The inner otlphttp exporter handles: protobuf serialization, compression,
// retry, connection pooling. Auth chaining is done via a custom
// cwlogsHeadersAuthExtension that wraps sigv4auth and adds dynamic headers.
package cwotlplogs

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
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/otlphttpexporter"
	"go.opentelemetry.io/collector/extension/extensionauth"
	"go.opentelemetry.io/collector/pdata/plog"
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
	logger   *zap.Logger
	cfg      *Config
	settings exporter.Settings

	innerExporter exporter.Logs

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
		settings:       settings,
		failureBackoff: backoff,
	}, nil
}

// start creates the inner otlphttp exporter. It replaces the configured auth
// extension with a wrapper that reads x-aws-log-group from client.Metadata
// and delegates to the original auth (sigv4auth) for signing.
func (e *cwOTLPLogsExporter) start(ctx context.Context, host component.Host) error {
	// Resolve the original auth extension (sigv4auth) so we can wrap it
	var innerAuth extensionauth.HTTPClient
	if e.cfg.ClientConfig.Auth != nil {
		ext, ok := host.GetExtensions()[e.cfg.ClientConfig.Auth.AuthenticatorID]
		if !ok {
			return fmt.Errorf("auth extension %q not found", e.cfg.ClientConfig.Auth.AuthenticatorID)
		}
		httpClient, ok := ext.(extensionauth.HTTPClient)
		if !ok {
			return fmt.Errorf("auth extension %q does not implement HTTPClient", e.cfg.ClientConfig.Auth.AuthenticatorID)
		}
		innerAuth = httpClient
	}

	// Register a temporary auth extension that wraps sigv4auth with header injection.
	// We do this by creating a custom host wrapper that returns our modified extension map.
	wrappedHost := &authOverrideHost{
		Host:      host,
		authID:    e.cfg.ClientConfig.Auth.AuthenticatorID,
		innerAuth: innerAuth,
	}

	// Build otlphttp config from our config
	otlpCfg := otlphttpexporter.NewFactory().CreateDefaultConfig().(*otlphttpexporter.Config)
	otlpCfg.ClientConfig = e.cfg.ClientConfig
	otlpCfg.RetryConfig = e.cfg.RetryConfig
	otlpCfg.QueueConfig = e.cfg.QueueConfig
	otlpCfg.LogsEndpoint = e.cfg.LogsEndpoint

	// Create inner otlphttp logs exporter with correct component ID
	factory := otlphttpexporter.NewFactory()
	innerSettings := e.settings
	innerSettings.ID = component.NewID(component.MustNewType("otlphttp"))
	inner, err := factory.CreateLogs(ctx, innerSettings, otlpCfg)
	if err != nil {
		return fmt.Errorf("failed to create inner otlphttp exporter: %w", err)
	}

	// Start the inner exporter with the wrapped host (so it gets our auth override)
	if err := inner.Start(ctx, wrappedHost); err != nil {
		return fmt.Errorf("failed to start inner otlphttp exporter: %w", err)
	}

	e.innerExporter = inner
	e.logger.Info("cwotlplogs wrapper started with inner otlphttp exporter")
	return nil
}

func (e *cwOTLPLogsExporter) shutdown(ctx context.Context) error {
	if e.innerExporter != nil {
		return e.innerExporter.Shutdown(ctx)
	}
	return nil
}

// pushLogs groups ResourceLogs by service.name, ensures log groups exist,
// and forwards each group to the inner otlphttp exporter with client.Metadata
// set for dynamic header injection.
func (e *cwOTLPLogsExporter) pushLogs(ctx context.Context, ld plog.Logs) error {
	groups := e.groupByService(ld)

	var errs []error
	for logGroup, entry := range groups {
		logStream := e.cfg.LogStreamName
		if logStream == "" {
			logStream = "default"
		}

		if e.cfg.AutoCreate {
			e.ensureProvisioned(logGroup, logStream)
		}

		// Set log_group and log_stream on client.Metadata.
		// The cwlogsHeadersRoundTripper reads these and sets HTTP headers.
		md := client.NewMetadata(map[string][]string{
			"log_group":  {logGroup},
			"log_stream": {logStream},
		})
		cl := client.FromContext(ctx)
		cl.Metadata = md
		exportCtx := client.NewContext(ctx, cl)

		if err := e.innerExporter.ConsumeLogs(exportCtx, entry.logs); err != nil {
			errs = append(errs, fmt.Errorf("log group %q: %w", logGroup, err))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("failed to send to %d log groups: %v", len(errs), errs)
	}
	return nil
}

type serviceGroup struct {
	logs plog.Logs
}

func (e *cwOTLPLogsExporter) groupByService(ld plog.Logs) map[string]*serviceGroup {
	groups := make(map[string]*serviceGroup)

	for i := 0; i < ld.ResourceLogs().Len(); i++ {
		rl := ld.ResourceLogs().At(i)
		serviceName := e.cfg.DefaultServiceName

		if val, exists := rl.Resource().Attributes().Get("service.name"); exists {
			if s := val.AsString(); s != "" {
				serviceName = s
			}
		}

		logGroup := strings.ReplaceAll(e.cfg.LogGroupName, "{ServiceName}", serviceName)

		group, exists := groups[logGroup]
		if !exists {
			group = &serviceGroup{logs: plog.NewLogs()}
			groups[logGroup] = group
		}
		rl.CopyTo(group.logs.ResourceLogs().AppendEmpty())
	}

	return groups
}

func (e *cwOTLPLogsExporter) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

// --- Auth override host ---
// authOverrideHost wraps component.Host and replaces the auth extension with
// a version that injects x-aws-log-group/x-aws-log-stream from client.Metadata
// before delegating to the original auth (sigv4auth) for signing.

type authOverrideHost struct {
	component.Host
	authID    component.ID
	innerAuth extensionauth.HTTPClient
}

func (h *authOverrideHost) GetExtensions() map[component.ID]component.Component {
	exts := make(map[component.ID]component.Component, len(h.Host.GetExtensions()))
	for id, ext := range h.Host.GetExtensions() {
		exts[id] = ext
	}
	// Replace the auth extension with our wrapper
	exts[h.authID] = &cwlogsHeadersAuthExtension{inner: h.innerAuth}
	return exts
}

// cwlogsHeadersAuthExtension wraps an inner auth (sigv4auth) and adds
// x-aws-log-group/x-aws-log-stream headers from client.Metadata before signing.
type cwlogsHeadersAuthExtension struct {
	component.StartFunc
	component.ShutdownFunc
	inner extensionauth.HTTPClient
}

func (e *cwlogsHeadersAuthExtension) RoundTripper(base http.RoundTripper) (http.RoundTripper, error) {
	// Chain: our header injection -> sigv4auth signing -> base transport
	sigv4Transport := base
	if e.inner != nil {
		var err error
		sigv4Transport, err = e.inner.RoundTripper(base)
		if err != nil {
			return nil, err
		}
	}
	return &cwlogsHeadersRoundTripper{base: sigv4Transport}, nil
}

// cwlogsHeadersRoundTripper reads log_group/log_stream from client.Metadata
// and sets them as x-aws-log-group/x-aws-log-stream headers.
type cwlogsHeadersRoundTripper struct {
	base http.RoundTripper
}

func (rt *cwlogsHeadersRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	cl := client.FromContext(req.Context())

	if groups := cl.Metadata.Get("log_group"); len(groups) > 0 {
		req2 := req.Clone(req.Context())
		req2.Header.Set("x-aws-log-group", groups[0])
		if streams := cl.Metadata.Get("log_stream"); len(streams) > 0 {
			req2.Header.Set("x-aws-log-stream", streams[0])
		}
		return rt.base.RoundTrip(req2)
	}

	return rt.base.RoundTrip(req)
}

// --- Log group provisioning (same pattern as POC 1 and 2) ---

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

	jitter := time.Duration(rand.Int63n(int64(500 * time.Millisecond))) // nolint:gosec
	time.Sleep(jitter)

	e.logger.Info("Creating log group/stream",
		zap.String("logGroup", logGroup),
		zap.String("logStream", logStream),
		zap.String("region", region),
	)

	err := createLogGroupAndStream(region, logGroup, logStream)
	if err != nil {
		e.provisioned.Store(key, &provisionStatus{success: false, timestamp: time.Now()})
		e.logger.Warn("Failed to create log group/stream",
			zap.String("logGroup", logGroup),
			zap.String("logStream", logStream),
			zap.Duration("backoff", e.failureBackoff),
			zap.Error(err),
		)
		return
	}

	e.provisioned.Store(key, &provisionStatus{success: true, timestamp: time.Now()})
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
	sess, err := session.NewSession(&aws.Config{Region: aws.String(region)})
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
