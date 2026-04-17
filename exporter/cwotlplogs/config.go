// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT

package cwotlplogs

import (
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/config/configretry"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
)

// Config for the cwotlplogs exporter.
// Embeds otlphttp's config fields so the inner exporter can be configured
// transparently (endpoint, auth, compression, retry, etc.).
type Config struct {
	confighttp.ClientConfig        `mapstructure:",squash"`
	QueueConfig                    exporterhelper.QueueBatchConfig `mapstructure:"sending_queue"`
	RetryConfig                    configretry.BackOffConfig       `mapstructure:"retry_on_failure"`

	// LogsEndpoint overrides the URL for logs. If omitted, Endpoint + "/v1/logs" is used.
	LogsEndpoint string `mapstructure:"logs_endpoint"`

	// LogGroupName is the log group name template. Use {ServiceName} as a
	// placeholder for the service.name resource attribute value.
	LogGroupName string `mapstructure:"log_group_name"`

	// LogStreamName is the static log stream name.
	LogStreamName string `mapstructure:"log_stream_name"`

	// DefaultServiceName is the fallback when service.name is not present.
	DefaultServiceName string `mapstructure:"default_service_name"`

	// AutoCreate enables automatic log group/stream creation.
	AutoCreate bool `mapstructure:"auto_create"`

	// Region overrides the AWS region for CreateLogGroup/CreateLogStream calls.
	// If empty, extracted from the endpoint URL.
	Region string `mapstructure:"region,omitempty"`

	// FailureBackoffSeconds is the TTL for negative cache entries.
	FailureBackoffSeconds int `mapstructure:"failure_backoff_seconds,omitempty"`
}
