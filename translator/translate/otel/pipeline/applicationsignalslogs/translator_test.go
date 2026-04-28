// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT

package applicationsignalslogs

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/pipeline"

	"github.com/aws/amazon-cloudwatch-agent/internal/util/collections"
	"github.com/aws/amazon-cloudwatch-agent/translator/translate/otel/common"
)

func TestTranslatorID(t *testing.T) {
	tt := NewTranslator()
	assert.Equal(t, "logs/application_signals_logs", tt.ID().String())
}

func TestTranslatorMissingKey(t *testing.T) {
	tt := NewTranslator()
	conf := confmap.NewFromStringMap(map[string]interface{}{})
	got, err := tt.Translate(conf)
	assert.Nil(t, got)
	assert.Equal(t, &common.MissingKeyError{
		ID:      pipeline.NewIDWithName(pipeline.SignalLogs, pipelineName),
		JsonKey: fmt.Sprint(common.AppSignalsLogs),
	}, err)
}

func TestTranslatorWithAppSignalsLogs(t *testing.T) {
	tt := NewTranslator()
	conf := confmap.NewFromStringMap(map[string]interface{}{
		"logs": map[string]interface{}{
			"logs_collected": map[string]interface{}{
				"application_signals": map[string]interface{}{},
			},
		},
	})
	got, err := tt.Translate(conf)
	require.NoError(t, err)
	require.NotNil(t, got)

	// Verify processors: transform → attributestocontext → batch
	assert.Equal(t, []string{
		"transform/application_signals_logs",
		"attributestocontext",
		"batch/application_signals_logs",
	}, collections.MapSlice(got.Processors.Keys(), component.ID.String))

	// Verify exporters
	assert.Equal(t, []string{"otlphttp/appsignals_logs"},
		collections.MapSlice(got.Exporters.Keys(), component.ID.String))

	// Verify extensions
	assert.Equal(t, []string{
		"sigv4auth/appsignals_logs",
		"awscloudwatchlogsprovisioner",
		"agenthealth/logs",
	}, collections.MapSlice(got.Extensions.Keys(), component.ID.String))
}

func TestTranslatorWithDebug(t *testing.T) {
	tt := NewTranslator()
	conf := confmap.NewFromStringMap(map[string]interface{}{
		"agent": map[string]interface{}{
			"debug": true,
		},
		"logs": map[string]interface{}{
			"logs_collected": map[string]interface{}{
				"application_signals": map[string]interface{}{},
			},
		},
	})
	got, err := tt.Translate(conf)
	require.NoError(t, err)
	require.NotNil(t, got)

	exporters := collections.MapSlice(got.Exporters.Keys(), component.ID.String)
	assert.Contains(t, exporters, "debug/application_signals_logs")
	assert.Contains(t, exporters, "otlphttp/appsignals_logs")
}

func TestTranslatorWithCustomLogGroup(t *testing.T) {
	tt := NewTranslator()
	conf := confmap.NewFromStringMap(map[string]interface{}{
		"logs": map[string]interface{}{
			"logs_collected": map[string]interface{}{
				"application_signals": map[string]interface{}{
					"log_group_name":  "/custom/{service.name}",
					"log_stream_name": "custom-stream",
				},
			},
		},
	})
	got, err := tt.Translate(conf)
	require.NoError(t, err)
	require.NotNil(t, got)

	// Dynamic path: transform + attributestocontext + batch(metadata_keys)
	processors := collections.MapSlice(got.Processors.Keys(), component.ID.String)
	assert.Contains(t, processors, "transform/application_signals_logs")
	assert.Contains(t, processors, "attributestocontext")

	assert.Contains(t,
		collections.MapSlice(got.Extensions.Keys(), component.ID.String),
		"awscloudwatchlogsprovisioner")
}

func TestTranslatorStaticLogGroup(t *testing.T) {
	tt := NewTranslator()
	conf := confmap.NewFromStringMap(map[string]interface{}{
		"logs": map[string]interface{}{
			"logs_collected": map[string]interface{}{
				"application_signals": map[string]interface{}{
					"log_group_name":  "/static/my-app",
					"log_stream_name": "my-stream",
				},
			},
		},
	})
	got, err := tt.Translate(conf)
	require.NoError(t, err)
	require.NotNil(t, got)

	// Static path: only batch, no transform or attributestocontext
	processors := collections.MapSlice(got.Processors.Keys(), component.ID.String)
	assert.Equal(t, []string{"batch/application_signals_logs"}, processors)
	assert.NotContains(t, processors, "transform/application_signals_logs")
	assert.NotContains(t, processors, "attributestocontext")

	assert.Contains(t,
		collections.MapSlice(got.Extensions.Keys(), component.ID.String),
		"awscloudwatchlogsprovisioner")
}

func TestTranslatorWithFallbackKey(t *testing.T) {
	tt := NewTranslator()
	conf := confmap.NewFromStringMap(map[string]interface{}{
		"logs": map[string]interface{}{
			"logs_collected": map[string]interface{}{
				"app_signals": map[string]interface{}{},
			},
		},
	})
	got, err := tt.Translate(conf)
	require.NoError(t, err)
	require.NotNil(t, got)
}

func TestParseTemplate(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []templateSegment
	}{
		{
			name:     "pure literal",
			input:    "/static/group",
			expected: []templateSegment{{literal: "/static/group"}},
		},
		{
			name:  "single placeholder",
			input: "/prefix/{service.name}",
			expected: []templateSegment{
				{literal: "/prefix/"},
				{attribute: "service.name"},
			},
		},
		{
			name:  "placeholder at start",
			input: "{service.name}/suffix",
			expected: []templateSegment{
				{attribute: "service.name"},
				{literal: "/suffix"},
			},
		},
		{
			name:  "multiple placeholders",
			input: "/a/{attr.one}/b/{attr.two}/c",
			expected: []templateSegment{
				{literal: "/a/"},
				{attribute: "attr.one"},
				{literal: "/b/"},
				{attribute: "attr.two"},
				{literal: "/c"},
			},
		},
		{
			name:  "adjacent placeholders",
			input: "{attr.one}{attr.two}",
			expected: []templateSegment{
				{attribute: "attr.one"},
				{attribute: "attr.two"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parseTemplate(tt.input)
			assert.Equal(t, tt.expected, got)
		})
	}
}

func TestBuildOTTLSetStatement(t *testing.T) {
	tests := []struct {
		name     string
		segments []templateSegment
		expected string
	}{
		{
			name:     "pure literal",
			segments: []templateSegment{{literal: "my-stream"}},
			expected: `set(resource.attributes["cwlogs.log_stream"], "my-stream") where resource.attributes["cwlogs.log_stream"] == nil`,
		},
		{
			name: "single placeholder with prefix",
			segments: []templateSegment{
				{literal: "/prefix/"},
				{attribute: "service.name"},
			},
			expected: `set(resource.attributes["cwlogs.log_stream"], Concat(["/prefix/", resource.attributes["service.name"]], "")) where resource.attributes["cwlogs.log_stream"] == nil`,
		},
		{
			name: "multiple placeholders",
			segments: []templateSegment{
				{literal: "/a/"},
				{attribute: "attr.one"},
				{literal: "/b/"},
				{attribute: "attr.two"},
			},
			expected: `set(resource.attributes["cwlogs.log_stream"], Concat(["/a/", resource.attributes["attr.one"], "/b/", resource.attributes["attr.two"]], "")) where resource.attributes["cwlogs.log_stream"] == nil`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := buildOTTLSetStatement(metadataKeyLogStream, tt.segments)
			assert.Equal(t, tt.expected, got)
		})
	}
}

func TestResolveLogConfig(t *testing.T) {
	tests := []struct {
		name                string
		logGroupName        string
		logStreamName       string
		expectGroupTemplate []templateSegment
		expectStreamTemplate []templateSegment
	}{
		{
			name: "default (no config)",
			expectGroupTemplate: []templateSegment{
				{literal: defaultLogGroupPrefix},
				{attribute: "service.name"},
			},
			expectStreamTemplate: []templateSegment{{literal: defaultLogStreamName}},
		},
		{
			name:         "with placeholder",
			logGroupName: "/custom/prefix/{service.name}",
			expectGroupTemplate: []templateSegment{
				{literal: "/custom/prefix/"},
				{attribute: "service.name"},
			},
			expectStreamTemplate: []templateSegment{{literal: defaultLogStreamName}},
		},
		{
			name:          "static group and stream (no placeholders)",
			logGroupName:  "/static/group",
			logStreamName: "my-stream",
			expectGroupTemplate:  []templateSegment{{literal: "/static/group"}},
			expectStreamTemplate: []templateSegment{{literal: "my-stream"}},
		},
		{
			name:         "multiple placeholders in group",
			logGroupName: "/a/{attr.one}/b/{service.name}",
			expectGroupTemplate: []templateSegment{
				{literal: "/a/"},
				{attribute: "attr.one"},
				{literal: "/b/"},
				{attribute: "service.name"},
			},
			expectStreamTemplate: []templateSegment{{literal: defaultLogStreamName}},
		},
		{
			name:          "placeholders in both group and stream",
			logGroupName:  "/logs/{service.name}",
			logStreamName: "{host.name}/{service.instance.id}",
			expectGroupTemplate: []templateSegment{
				{literal: "/logs/"},
				{attribute: "service.name"},
			},
			expectStreamTemplate: []templateSegment{
				{attribute: "host.name"},
				{literal: "/"},
				{attribute: "service.instance.id"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfgMap := map[string]interface{}{
				"logs": map[string]interface{}{
					"logs_collected": map[string]interface{}{
						"application_signals": map[string]interface{}{},
					},
				},
			}
			if tt.logGroupName != "" {
				appSignalsCfg := map[string]interface{}{
					"log_group_name": tt.logGroupName,
				}
				if tt.logStreamName != "" {
					appSignalsCfg["log_stream_name"] = tt.logStreamName
				}
				cfgMap["logs"].(map[string]interface{})["logs_collected"].(map[string]interface{})["application_signals"] = appSignalsCfg
			}
			conf := confmap.NewFromStringMap(cfgMap)
			configKeys := common.AppSignalsConfigKeys[pipeline.SignalLogs]

			groupTemplate, streamTemplate := resolveLogConfig(conf, configKeys)
			assert.Equal(t, tt.expectGroupTemplate, groupTemplate)
			assert.Equal(t, tt.expectStreamTemplate, streamTemplate)
		})
	}
}

func TestAutoEnableIfNeeded(t *testing.T) {
	t.Run("MetricsConfigured_LogsNotConfigured", func(t *testing.T) {
		conf := map[string]interface{}{
			"logs": map[string]interface{}{
				"metrics_collected": map[string]interface{}{
					"application_signals": map[string]interface{}{},
				},
			},
		}
		AutoEnableIfNeeded(conf)
		logs := conf["logs"].(map[string]interface{})
		logsCollected := logs["logs_collected"].(map[string]interface{})
		_, exists := logsCollected["application_signals"]
		assert.True(t, exists, "should auto-enable application_signals in logs_collected")
	})

	t.Run("MetricsConfigured_LogsAlreadyConfigured", func(t *testing.T) {
		conf := map[string]interface{}{
			"logs": map[string]interface{}{
				"metrics_collected": map[string]interface{}{
					"application_signals": map[string]interface{}{},
				},
				"logs_collected": map[string]interface{}{
					"application_signals": map[string]interface{}{
						"log_group_name": "/custom/group",
					},
				},
			},
		}
		AutoEnableIfNeeded(conf)
		logs := conf["logs"].(map[string]interface{})
		logsCollected := logs["logs_collected"].(map[string]interface{})
		as := logsCollected["application_signals"].(map[string]interface{})
		assert.Equal(t, "/custom/group", as["log_group_name"], "should not override existing config")
	})

	t.Run("MetricsNotConfigured", func(t *testing.T) {
		conf := map[string]interface{}{
			"logs": map[string]interface{}{},
		}
		AutoEnableIfNeeded(conf)
		logs := conf["logs"].(map[string]interface{})
		_, exists := logs["logs_collected"]
		assert.False(t, exists, "should not auto-enable without metrics")
	})

	t.Run("FallbackKey_AppSignals", func(t *testing.T) {
		conf := map[string]interface{}{
			"logs": map[string]interface{}{
				"metrics_collected": map[string]interface{}{
					"app_signals": map[string]interface{}{},
				},
			},
		}
		AutoEnableIfNeeded(conf)
		logs := conf["logs"].(map[string]interface{})
		logsCollected := logs["logs_collected"].(map[string]interface{})
		_, exists := logsCollected["application_signals"]
		assert.True(t, exists, "should auto-enable with fallback key")
	})
}
