package router

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"bridge-kafka-service/testutil/mocksarama"
)

func TestPause_ToggleAppliesToEveryRegisteredGroup(t *testing.T) {
	p := NewPause()
	a, b := mocksarama.NewFakeConsumerGroup(nil), mocksarama.NewFakeConsumerGroup(nil)
	p.Register(a)
	p.Register(b)

	p.Toggle()
	assert.True(t, p.Paused())
	assert.Equal(t, 1, a.PauseCount())
	assert.Equal(t, 1, b.PauseCount())

	p.Unregister(b)
	p.Toggle()
	assert.False(t, p.Paused())
	assert.Equal(t, 1, a.ResumeCount())
	assert.Zero(t, b.ResumeCount(), "an unregistered group no longer follows")
}

func TestPause_PauseClaimOnlyWhilePaused(t *testing.T) {
	p := NewPause()
	g := mocksarama.NewFakeConsumerGroup(nil)

	p.PauseClaim(g, "in", 0)
	assert.Zero(t, g.PauseCount(), "a running process does not pause new claims")

	p.Toggle()
	p.PauseClaim(g, "in", 1)
	assert.Equal(t, 1, g.PauseCount(), "a claim started while paused pauses its partition")
}

func TestPause_Wait(t *testing.T) {
	p := NewPause()
	assert.True(t, p.Wait(context.Background()), "a running process does not block")

	p.Toggle()
	got := make(chan bool, 1)
	go func() { got <- p.Wait(context.Background()) }()
	select {
	case <-got:
		t.Fatal("Wait must block while paused")
	case <-time.After(50 * time.Millisecond):
	}
	p.Toggle()
	select {
	case ok := <-got:
		assert.True(t, ok)
	case <-time.After(time.Second):
		t.Fatal("Wait did not return on resume")
	}

	p.Toggle()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	assert.False(t, p.Wait(ctx), "a context that ends returns false")
}

func TestPause_NilIsNeverPaused(t *testing.T) {
	var p *Pause
	g := mocksarama.NewFakeConsumerGroup(nil)

	p.Toggle()
	p.Register(g)
	p.PauseClaim(g, "in", 0)

	assert.False(t, p.Paused())
	assert.True(t, p.Wait(context.Background()))
	assert.Zero(t, g.PauseCount())
}

func TestPause_ConcurrentToggleAndClaims(t *testing.T) {
	p := NewPause()
	g := mocksarama.NewFakeConsumerGroup(nil)
	p.Register(g)
	var wg sync.WaitGroup
	for i := range 16 {
		wg.Add(2)
		go func() { defer wg.Done(); p.Toggle() }()
		go func() { defer wg.Done(); p.PauseClaim(g, "in", int32(i)); _ = p.Paused() }()
	}
	wg.Wait()

	assert.False(t, p.Paused(), "an even number of toggles leaves the process running")
}
