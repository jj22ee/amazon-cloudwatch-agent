// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT

// Package applicationsignalslogs translates logs.logs_collected.application_signals
// into an OTel logs pipeline that routes OTLP logs to CloudWatch via the CW OTLP
// endpoint with dynamic per-service log group routing.
package applicationsignalslogs

import (
	"fmt"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/pipeline"

	"github.com/aws/amazon-cloudwatch-agent/translator/translate/otel/common"
	"github.com/aws/amazon-cloudwatch-agent/translator/translate/otel/extension/agenthealth"
	"github.com/aws/amazon-cloudwatch-agent/translator/translate/otel/exporter/debug"
	"github.com/aws/amazon-cloudwatch-agent/translator/translate/otel/receiver/otlp"
)

const (
	pipelineName = "application_signals_logs"
	defaultLogGroupName = "/telemend/telemetry/{service.name}"
)

type translator struct{}

var _ common.PipelineTranslator = (*translator)(nil)

func NewTranslator() common.PipelineTranslator {
	return &translator{}
}

func (t *translator) ID() pipeline.ID {
	return pipeline.NewIDWithName(pipeline.SignalLogs, pipelineName)
}

func (t *translator) Translate(conf *confmap.Conf) (*common.ComponentTranslators, error) {
	configKeys := common.AppSignalsConfigKeys[pipeline.SignalLogs]
	if conf == nil || (!conf.IsSet(configKeys[0]) && !conf.IsSet(configKeys[1])) {
		return nil, &common.MissingKeyError{ID: t.ID(), JsonKey: configKeys[0]}
	}

	// Read config values with defaults
	logGroupName := defaultLogGroupName
	logStreamName := "default"

	for _, key := range configKeys {
		if v, ok := common.GetString(conf, common.ConfigKey(key, "log_group_name")); ok {
			logGroupName = v
		}
		if v, ok := common.GetString(conf, common.ConfigKey(key, "log_stream_name")); ok {
			logStreamName = v
		}
	}

	translators := &common.ComponentTranslators{
		Receivers:  otlp.NewTranslators(conf, common.AppSignals, pipeline.SignalLogs.String()),
		Processors: common.NewTranslatorMap[component.Config, component.ID](),
		Exporters:  common.NewTranslatorMap[component.Config, component.ID](),
		Extensions: common.NewTranslatorMap[component.Config, component.ID](),
	}

	// Processors: attributestocontext + batch (with metadata_keys)
	translators.Processors.Set(newAttributesToContextTranslator())
	translators.Processors.Set(newBatchTranslator())

	// Exporter: otlphttp pointing to CW OTLP endpoint with provisioner as auth
	translators.Exporters.Set(newOTLPHTTPExporterTranslator())

	if enabled, _ := common.GetBool(conf, common.AgentDebugConfigKey); enabled {
		translators.Exporters.Set(debug.NewTranslator(common.WithName(pipelineName)))
	}

	// Extensions: sigv4auth + awscloudwatchlogsprovisioner
	translators.Extensions.Set(newSigV4AuthTranslator())
	translators.Extensions.Set(newProvisionerTranslator(logGroupName, logStreamName))
	translators.Extensions.Set(agenthealth.NewTranslator(agenthealth.LogsName, []string{agenthealth.OperationPutLogEvents}))

	return translators, nil
}

// getAppSignalsLogsConfigKey returns the active config key for application_signals logs.
func getAppSignalsLogsConfigKey(conf *confmap.Conf) string {
	configKeys := common.AppSignalsConfigKeys[pipeline.SignalLogs]
	for _, key := range configKeys {
		if conf.IsSet(key) {
			return key
		}
	}
	return configKeys[0]
}

// IsAppSignalsMetricsEnabled checks if logs.metrics_collected.application_signals is set.
func IsAppSignalsMetricsEnabled(conf *confmap.Conf) bool {
	metricsKeys := common.AppSignalsConfigKeys[pipeline.SignalMetrics]
	return conf.IsSet(metricsKeys[0]) || conf.IsSet(metricsKeys[1])
}

// AutoEnableIfNeeded injects logs.logs_collected.application_signals with defaults
// when logs.metrics_collected.application_signals is configured but
// logs.logs_collected.application_signals is not.
func AutoEnableIfNeeded(conf map[string]interface{}) {
	// Check if metrics is configured
	logs, ok := conf["logs"].(map[string]interface{})
	if !ok {
		return
	}
	metricsCollected, ok := logs["metrics_collected"].(map[string]interface{})
	if !ok {
		return
	}
	_, hasAppSignals := metricsCollected["application_signals"]
	_, hasAppSignalsFallback := metricsCollected["app_signals"]
	if !hasAppSignals && !hasAppSignalsFallback {
		return
	}

	// Metrics is configured — auto-enable logs if not already set
	logsCollected, ok := logs["logs_collected"].(map[string]interface{})
	if !ok {
		logsCollected = map[string]interface{}{}
		logs["logs_collected"] = logsCollected
	}
	if _, exists := logsCollected["application_signals"]; exists {
		return // already configured
	}
	if _, exists := logsCollected["app_signals"]; exists {
		return // already configured with fallback name
	}

	// Auto-enable with defaults
	logsCollected["application_signals"] = map[string]interface{}{}
	fmt.Println("I! Auto-enabling logs.logs_collected.application_signals (triggered by logs.metrics_collected.application_signals)")
}
