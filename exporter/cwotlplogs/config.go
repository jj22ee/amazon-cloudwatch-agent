// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT

package cwotlplogs

import (
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/config/configretry"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
)

// Config for the cwotlplogs exporter.
type Config struct {
	confighttp.ClientConfig         `mapstructure:",squash"`
	exporterhelper.QueueBatchConfig `mapstructure:",squash"`
	configretry.BackOffConfig       `mapstructure:"retry_on_failure"`

	// LogGroupName is the log group name template. Use {ServiceName} as a
	// placeholder for the service.name resource attribute value.
	// Example: "/test/telemetry/{ServiceName}"
	LogGroupName string `mapstructure:"log_group_name"`

	// LogStreamName is the static log stream name.
	// Default: "default"
	LogStreamName string `mapstructure:"log_stream_name"`

	// DefaultServiceName is the fallback when service.name is not present.
	// Default: "UnknownService"
	DefaultServiceName string `mapstructure:"default_service_name"`

	// AutoCreate enables automatic log group/stream creation.
	// Default: true
	AutoCreate bool `mapstructure:"auto_create"`

	// Region overrides the AWS region for CreateLogGroup/CreateLogStream calls.
	// If empty, extracted from the endpoint URL.
	Region string `mapstructure:"region,omitempty"`

	// FailureBackoffSeconds is the TTL for negative cache entries.
	// Default: 30 seconds.
	FailureBackoffSeconds int `mapstructure:"failure_backoff_seconds,omitempty"`
}
