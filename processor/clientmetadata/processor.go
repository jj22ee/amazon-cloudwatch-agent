// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT

// Package clientmetadata provides an OTel processor that copies resource
// attributes into client.Metadata on the pipeline context.
//
// This is the bridge that makes resource attributes available to extensions
// that use `from_context` (e.g., headers_setter). Without this processor,
// resource attributes are only accessible within the pipeline data (plog.Logs)
// but not in the request context used by auth extensions.
//
// Usage:
//
//	processors:
//	  clientmetadata:
//	    extractions:
//	      - key: service.name
//	        from_resource_attribute: service.name
//
// The processor reads the first ResourceLogs entry's resource attributes
// and sets matching values on client.Metadata. When used with batch processor's
// metadata_keys, each batch contains logs from only one service, so reading
// from the first ResourceLogs is unambiguous.
package clientmetadata

import (
	"context"

	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/zap"
)

type clientMetadataProcessor struct {
	logger       *zap.Logger
	cfg          *Config
	nextConsumer consumer.Logs

	component.StartFunc
	component.ShutdownFunc
}

func newProcessor(logger *zap.Logger, cfg *Config, nextConsumer consumer.Logs) *clientMetadataProcessor {
	return &clientMetadataProcessor{
		logger:       logger,
		cfg:          cfg,
		nextConsumer: nextConsumer,
	}
}

func (p *clientMetadataProcessor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (p *clientMetadataProcessor) ConsumeLogs(ctx context.Context, ld plog.Logs) error {
	if ld.ResourceLogs().Len() == 0 {
		return p.nextConsumer.ConsumeLogs(ctx, ld)
	}

	// Extract values from the first ResourceLogs entry.
	// When used with batch processor + metadata_keys, all ResourceLogs in the
	// batch share the same metadata key values, so the first entry is representative.
	resource := ld.ResourceLogs().At(0).Resource()
	attrs := resource.Attributes()

	mdMap := make(map[string][]string, len(p.cfg.Extractions))
	for _, extraction := range p.cfg.Extractions {
		val, exists := attrs.Get(extraction.FromResourceAttribute)
		if exists {
			mdMap[extraction.Key] = []string{val.AsString()}
		}
	}

	// Create a new context with the metadata attached.
	// client.Metadata is immutable — we construct a new one from a map.
	cl := client.FromContext(ctx)
	cl.Metadata = client.NewMetadata(mdMap)
	ctx = client.NewContext(ctx, cl)

	return p.nextConsumer.ConsumeLogs(ctx, ld)
}
