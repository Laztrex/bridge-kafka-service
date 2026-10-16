package router

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"bridge-kafka-service/testutil/mocksarama"
)

func TestProducerRegistry(t *testing.T) {
	reg := NewProducerRegistry()
	assert.False(t, reg.Ready())

	_, ok := reg.Lookup("a")
	assert.False(t, ok)

	p := mocksarama.NewSyncProducer(t)
	reg.Set(p, []string{"a", "b"})

	assert.True(t, reg.Ready())
	got, ok := reg.Lookup("b")
	assert.True(t, ok)
	assert.Same(t, p, got)
	_, ok = reg.Lookup("c")
	assert.False(t, ok, "topic list is an allow-list")
	assert.Len(t, reg.Producers(), 2)
}

func TestToggleConsumptionFlow(t *testing.T) {
	g := mocksarama.NewFakeConsumerGroup(mocksarama.NewFakeClient())
	paused := false

	toggleConsumptionFlow(g, &paused)
	assert.True(t, paused)
	assert.Equal(t, 1, g.PauseCount())

	toggleConsumptionFlow(g, &paused)
	assert.False(t, paused)
	assert.Equal(t, 1, g.ResumeCount())

	toggleConsumptionFlow(nil, &paused)
}
