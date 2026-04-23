// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT

package applicationsignalslogs

import (
	"fmt"
	"time"

	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/awscloudwatchlogsprovisionerextension"
	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/sigv4authextension"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/attributestocontextprocessor"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/processor/batchprocessor"

	"github.com/aws/amazon-cloudwatch-agent/translator/translate/agent"
	"github.com/aws/amazon-cloudwatch-agent/translator/translate/otel/common"
	"github.com/aws/amazon-cloudwatch-agent/translator/translate/otel/exporter/otlphttp"
)

// --- sigv4auth extension translator ---

type sigV4AuthTranslator struct {
	factory component.Factory
}

func newSigV4AuthTranslator() common.ComponentTranslator {
	return &sigV4AuthTranslator{factory: sigv4authextension.NewFactory()}
}

func (t *sigV4AuthTranslator) ID() component.ID {
	return component.NewIDWithName(t.factory.(component.Factory).Type(), "appsignals_logs")
}

func (t *sigV4AuthTranslator) Translate(_ *confmap.Conf) (component.Config, error) {
	cfg := sigv4authextension.NewFactory().CreateDefaultConfig().(*sigv4authextension.Config)
	cfg.Region = agent.Global_Config.Region
	cfg.Service = "logs"
	if agent.Global_Config.Role_arn != "" {
		cfg.AssumeRole = sigv4authextension.AssumeRole{
			ARN:       agent.Global_Config.Role_arn,
			STSRegion: agent.Global_Config.Region,
		}
	}
	return cfg, nil
}

// --- awscloudwatchlogsprovisioner extension translator ---

type provisionerTranslator struct {
	logGroupName  string
	logStreamName string
}

func newProvisionerTranslator(logGroupName, logStreamName string) common.ComponentTranslator {
	return &provisionerTranslator{logGroupName: logGroupName, logStreamName: logStreamName}
}

func (t *provisionerTranslator) ID() component.ID {
	return component.MustNewID("awscloudwatchlogsprovisioner")
}

func (t *provisionerTranslator) Translate(_ *confmap.Conf) (component.Config, error) {
	sigv4AuthID := component.NewIDWithName(component.MustNewType("sigv4auth"), "appsignals_logs")
	cfg := awscloudwatchlogsprovisionerextension.NewFactory().CreateDefaultConfig().(*awscloudwatchlogsprovisionerextension.Config)
	cfg.AdditionalAuth = &sigv4AuthID
	cfg.LogGroupName = t.logGroupName
	cfg.LogStreamName = t.logStreamName
	return cfg, nil
}

// --- attributestocontext processor translator ---

type attributesToContextTranslator struct{}

func newAttributesToContextTranslator() common.ComponentTranslator {
	return &attributesToContextTranslator{}
}

func (t *attributesToContextTranslator) ID() component.ID {
	return component.MustNewID("attributestocontext")
}

func (t *attributesToContextTranslator) Translate(_ *confmap.Conf) (component.Config, error) {
	cfg := attributestocontextprocessor.NewFactory().CreateDefaultConfig()
	cfgMap := map[string]interface{}{
		"actions": []interface{}{
			map[string]interface{}{
				"key":                     "service.name",
				"action":                  "upsert",
				"from_resource_attribute": "service.name",
			},
		},
	}
	if err := confmap.NewFromStringMap(cfgMap).Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("failed to configure attributestocontext: %w", err)
	}
	return cfg, nil
}

// --- batch processor translator (with metadata_keys) ---

type batchTranslator struct{}

func newBatchTranslator() common.ComponentTranslator {
	return &batchTranslator{}
}

func (t *batchTranslator) ID() component.ID {
	return component.NewIDWithName(component.MustNewType("batch"), pipelineName)
}

func (t *batchTranslator) Translate(_ *confmap.Conf) (component.Config, error) {
	cfg := batchprocessor.NewFactory().CreateDefaultConfig().(*batchprocessor.Config)
	cfg.MetadataKeys = []string{"service.name"}
	cfg.SendBatchSize = 100
	cfg.Timeout = 5 * time.Second
	return cfg, nil
}

// newOTLPHTTPExporterTranslator creates the otlphttp exporter translator
// using the awscloudwatchlogsprovisioner as the authenticator.
func newOTLPHTTPExporterTranslator() common.ComponentTranslator {
	provisionerID := component.MustNewID("awscloudwatchlogsprovisioner")
	return otlphttp.NewTranslatorWithName("appsignals_logs", otlphttp.WithAuthenticator(provisionerID))
}
