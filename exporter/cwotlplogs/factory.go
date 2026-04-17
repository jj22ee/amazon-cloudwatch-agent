// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT

package cwotlplogs

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configcompression"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/config/configretry"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
)

const typeStr = "cwotlplogs"

func NewFactory() exporter.Factory {
	return exporter.NewFactory(
		component.MustNewType(typeStr),
		createDefaultConfig,
		exporter.WithLogs(createLogsExporter, component.StabilityLevelAlpha),
	)
}

func createDefaultConfig() component.Config {
	clientConfig := confighttp.NewDefaultClientConfig()
	clientConfig.Compression = configcompression.TypeGzip

	return &Config{
		ClientConfig:          clientConfig,
		RetryConfig:           configretry.NewDefaultBackOffConfig(),
		QueueConfig:           exporterhelper.NewDefaultQueueConfig(),
		LogGroupName:          "/test/telemetry/{ServiceName}",
		LogStreamName:         "default",
		DefaultServiceName:    "UnknownService",
		AutoCreate:            true,
		FailureBackoffSeconds: 30,
	}
}

func createLogsExporter(
	ctx context.Context,
	settings exporter.Settings,
	cfg component.Config,
) (exporter.Logs, error) {
	oCfg := cfg.(*Config)

	exp, err := newExporter(settings, oCfg)
	if err != nil {
		return nil, err
	}

	return exporterhelper.NewLogs(
		ctx,
		settings,
		cfg,
		exp.pushLogs,
		exporterhelper.WithStart(exp.start),
		exporterhelper.WithShutdown(exp.shutdown),
		exporterhelper.WithRetry(oCfg.RetryConfig),
		exporterhelper.WithQueue(oCfg.QueueConfig),
	)
}
