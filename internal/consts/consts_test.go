package consts_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"bridge-kafka-service/internal/consts"
	"bridge-kafka-service/testutil/requiredheaders"
)

func TestRequiredHeaders_EmptyByDefault(t *testing.T) {
	assert.Empty(t, consts.RequiredHeaders)
	assert.False(t, consts.IsRequiredHeader(consts.HeaderRequestIdKey))
}

func TestIsRequiredHeader_IgnoresCase(t *testing.T) {
	requiredheaders.Set(t, consts.HeaderClientId)

	assert.True(t, consts.IsRequiredHeader("x-client-id"))
	assert.True(t, consts.IsRequiredHeader("X-Client-Id"))
	assert.False(t, consts.IsRequiredHeader(consts.HeaderRequestIdKey))
}

// Every metric name the service can write carries the service prefix and none keeps the former
// controller_kafka_ or bridge_service_ spelling (MET-2).
func TestMetricNamesCarryTheServicePrefix(t *testing.T) {
	events := map[consts.EventType]string{
		consts.EventProduce:         "produce",
		consts.EventConsume:         "consume",
		consts.EventRequestService:  "request_service",
		consts.EventResponseService: "response_service",
	}
	for event, name := range events {
		for _, metric := range []string{
			consts.MetricsSuccessMapName[event],
			consts.MetricsFailureMapName[event],
			consts.MetricsRequestMapName[event],
			consts.MetricsDurationMapName[event],
		} {
			assert.True(t, strings.HasPrefix(metric, consts.MetricPrefix+name+"_"), metric)
			assert.NotContains(t, metric, "controller_kafka_")
			assert.NotContains(t, metric, "bridge_service_")
		}
	}
	assert.Equal(t, "bridge_kafka_service_pod_cpu_usage", consts.FieldMetricCpuUsagePercent)
	assert.Equal(t, "bridge_kafka_service_pod_memory_usage", consts.FieldMetricRamUsageMb)
}
