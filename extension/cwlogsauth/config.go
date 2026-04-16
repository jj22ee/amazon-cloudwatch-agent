// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT

package cwlogsauth

import (
	"go.opentelemetry.io/collector/component"
)

// Config for the cwlogsauth extension.
type Config struct {
	// Auth is a reference to the inner auth extension (typically sigv4auth)
	// that this extension wraps for request signing.
	Auth *component.ID `mapstructure:"auth"`

	// Region overrides the AWS region for CreateLogGroup/CreateLogStream calls.
	// If empty, the region is extracted from the request URL (logs.<region>.amazonaws.com).
	Region string `mapstructure:"region,omitempty"`

	// LogGroupName is the log group name template. Use {ServiceName} as a
	// placeholder for the service.name resource attribute value.
	// Example: "/test/telemetry/{ServiceName}"
	// Default: "/test/telemetry/{ServiceName}"
	LogGroupName string `mapstructure:"log_group_name"`

	// LogStreamName is the log stream name.
	// Default: "default"
	LogStreamName string `mapstructure:"log_stream_name"`

	// DefaultServiceName is the fallback service name when service.name is
	// not present in client.Metadata.
	// Default: "UnknownService"
	DefaultServiceName string `mapstructure:"default_service_name,omitempty"`

	// FailureBackoffSeconds is the TTL for negative cache entries (failed creation attempts).
	// During this period, the extension won't retry creation for the same (group, stream) pair.
	// Default: 30 seconds.
	FailureBackoffSeconds int `mapstructure:"failure_backoff_seconds,omitempty"`
}
