// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT

package applicationsignalslogs

import (
	"fmt"
	"time"

	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/awscloudwatchlogsprovisionerextension"
	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/sigv4authextension"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/attributestocontextprocessor"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/transformprocessor"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/processor/batchprocessor"

	"github.com/aws/amazon-cloudwatch-agent/translator/translate/agent"
	"github.com/aws/amazon-cloudwatch-agent/translator/translate/otel/common"
	"github.com/aws/amazon-cloudwatch-agent/translator/translate/otel/exporter/otlphttp"
)

const (
	// Metadata keys used to pass log group/stream from attributestocontext
	// processor to the provisioner extension.
	metadataKeyLogGroup  = "cwlogs.log_group"
	metadataKeyLogStream = "cwlogs.log_stream"
)

// --- sigv4auth extension translator ---

type sigV4AuthTranslator struct{}

func newSigV4AuthTranslator() common.ComponentTranslator {
	return &sigV4AuthTranslator{}
}

func (t *sigV4AuthTranslator) ID() component.ID {
	return component.NewIDWithName(component.MustNewType("sigv4auth"), "appsignals_logs")
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

type provisionerTranslator struct{}

func newProvisionerTranslator() common.ComponentTranslator {
	return &provisionerTranslator{}
}

func (t *provisionerTranslator) ID() component.ID {
	return component.MustNewID("awscloudwatchlogsprovisioner")
}

func (t *provisionerTranslator) Translate(_ *confmap.Conf) (component.Config, error) {
	sigv4AuthID := component.NewIDWithName(component.MustNewType("sigv4auth"), "appsignals_logs")
	cfg := awscloudwatchlogsprovisionerextension.NewFactory().CreateDefaultConfig().(*awscloudwatchlogsprovisionerextension.Config)
	cfg.AdditionalAuth = &sigv4AuthID
	cfg.LogGroupContextKey = metadataKeyLogGroup
	cfg.LogStreamContextKey = metadataKeyLogStream
	return cfg, nil
}

// --- transform processor translator ---
// Builds full log group/stream names into resource attributes from service.name.
// E.g., service.name="pet-clinic" + prefix="/aws/telemetry/" → cwlogs.log_group="/aws/telemetry/pet-clinic"

type transformTranslator struct {
	logGroupPrefix string
	logStreamName  string
}

func newTransformTranslator(logGroupPrefix, logStreamName string) common.ComponentTranslator {
	return &transformTranslator{logGroupPrefix: logGroupPrefix, logStreamName: logStreamName}
}

func (t *transformTranslator) ID() component.ID {
	return component.NewIDWithName(component.MustNewType("transform"), pipelineName)
}

func (t *transformTranslator) Translate(_ *confmap.Conf) (component.Config, error) {
	// Build the OTTL statements that construct the full log group/stream names
	// as resource attributes, so attributestocontext can copy them to metadata.
	concatStmt := fmt.Sprintf(
		`set(resource.attributes["%s"], Concat(["%s", resource.attributes["service.name"]], ""))`,
		metadataKeyLogGroup, t.logGroupPrefix,
	)
	streamStmt := fmt.Sprintf(
		`set(resource.attributes["%s"], "%s")`,
		metadataKeyLogStream, t.logStreamName,
	)

	cfgMap := map[string]interface{}{
		"log_statements": []interface{}{
			map[string]interface{}{
				"context": "resource",
				"statements": []interface{}{
					concatStmt,
					streamStmt,
				},
			},
		},
	}

	cfg := transformprocessor.NewFactory().CreateDefaultConfig()
	if err := confmap.NewFromStringMap(cfgMap).Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("failed to configure transform processor: %w", err)
	}
	return cfg, nil
}

// --- attributestocontext processor translator ---
// Copies cwlogs.log_group and cwlogs.log_stream from resource attributes to
// client.Metadata, making them available to the provisioner extension.

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
				"key":                     metadataKeyLogGroup,
				"action":                  "upsert",
				"from_resource_attribute": metadataKeyLogGroup,
			},
			map[string]interface{}{
				"key":                     metadataKeyLogStream,
				"action":                  "upsert",
				"from_resource_attribute": metadataKeyLogStream,
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
	// Both keys must be in metadata_keys — the batch processor creates a fresh
	// context containing only the listed keys, discarding all other metadata.
	// The provisioner extension needs both cwlogs.log_group and cwlogs.log_stream.
	cfg.MetadataKeys = []string{metadataKeyLogGroup, metadataKeyLogStream}
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
