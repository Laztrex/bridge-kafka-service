package router

import (
	"context"
	"sync"

	"github.com/IBM/sarama"
	"github.com/rs/zerolog/log"
)

// Pause is the process-wide pause state that SIGUSR1 toggles and every pipeline follows. All sarama
// pause and resume calls happen under its mutex, so a claim starting during a toggle can never end
// up paused while the process runs, or the reverse. A nil *Pause is never paused.
type Pause struct {
	mu      sync.Mutex
	paused  bool
	resumed chan struct{} // closed when the process runs; replaced when it pauses
	groups  map[sarama.ConsumerGroup]struct{}
}

// NewPause returns a running (not paused) pause state.
func NewPause() *Pause {
	resumed := make(chan struct{})
	close(resumed)
	return &Pause{resumed: resumed, groups: map[sarama.ConsumerGroup]struct{}{}}
}

// Toggle flips the state and applies it to every registered consumer group.
func (p *Pause) Toggle() {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.paused = !p.paused
	if p.paused {
		p.resumed = make(chan struct{})
		for g := range p.groups {
			g.PauseAll()
		}
		log.Info().Msg("pausing consumption")
		return
	}
	for g := range p.groups {
		g.ResumeAll()
	}
	close(p.resumed)
	log.Info().Msg("resuming consumption")
}

// Register makes group follow later toggles; Unregister stops it.
func (p *Pause) Register(group sarama.ConsumerGroup) {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.groups[group] = struct{}{}
}

// Unregister removes group, before it is closed.
func (p *Pause) Unregister(group sarama.ConsumerGroup) {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.groups, group)
}

// PauseClaim pauses one partition of group while the process is paused. A claim calls it when it
// starts, once its partition consumer exists, so partitions assigned during a pause stay paused.
func (p *Pause) PauseClaim(group sarama.ConsumerGroup, topic string, partition int32) {
	if p == nil || group == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.paused {
		group.Pause(map[string][]int32{topic: {partition}})
	}
}

// Wait blocks while the process is paused: true once it runs, false if ctx ends first.
func (p *Pause) Wait(ctx context.Context) bool {
	if p == nil {
		return true
	}
	for {
		p.mu.Lock()
		paused, resumed := p.paused, p.resumed
		p.mu.Unlock()
		if !paused {
			return true
		}
		select {
		case <-resumed:
		case <-ctx.Done():
			return false
		}
	}
}

// Paused reports whether the process is paused.
func (p *Pause) Paused() bool {
	if p == nil {
		return false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.paused
}
