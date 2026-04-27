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

	// Verify the transform processor is present (handles the custom prefix)
	processors := collections.MapSlice(got.Processors.Keys(), component.ID.String)
	assert.Contains(t, processors, "transform/application_signals_logs")

	// Verify the provisioner is configured
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

func TestResolveLogConfig(t *testing.T) {
	tests := []struct {
		name           string
		logGroupName   string
		logStreamName  string
		expectPrefix   string
		expectStream   string
	}{
		{
			name:         "default (no config)",
			expectPrefix: defaultLogGroupPrefix,
			expectStream: defaultLogStreamName,
		},
		{
			name:         "with placeholder",
			logGroupName: "/custom/prefix/{service.name}",
			expectPrefix: "/custom/prefix/",
			expectStream: defaultLogStreamName,
		},
		{
			name:          "static group (no placeholder)",
			logGroupName:  "/static/group",
			logStreamName: "my-stream",
			expectPrefix:  "/static/group",
			expectStream:  "my-stream",
		},
		{
			name:         "placeholder at start",
			logGroupName: "{service.name}/suffix",
			expectPrefix: "",
			expectStream: defaultLogStreamName,
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

			prefix, stream := resolveLogConfig(conf, configKeys)
			assert.Equal(t, tt.expectPrefix, prefix)
			assert.Equal(t, tt.expectStream, stream)
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
