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

	assert.Equal(t, []string{"otlp/grpc_0_0_0_0_4315", "otlp/http_0_0_0_0_4316"},
		collections.MapSlice(got.Receivers.Keys(), component.ID.String))
	assert.Equal(t, []string{"attributestocontext", "batch/application_signals_logs"},
		collections.MapSlice(got.Processors.Keys(), component.ID.String))
	assert.Equal(t, []string{"otlphttp/appsignals_logs"},
		collections.MapSlice(got.Exporters.Keys(), component.ID.String))
	assert.Equal(t, []string{"sigv4auth/appsignals_logs", "awscloudwatchlogsprovisioner", "agenthealth/logs"},
		collections.MapSlice(got.Extensions.Keys(), component.ID.String))
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

	// The provisioner should be configured with the custom log group
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
