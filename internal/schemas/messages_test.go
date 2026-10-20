package schemas

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConsumerEvent_AckHandoff(t *testing.T) {
	calls := 0
	src := &ConsumerEvent{}
	src.SetAck(func() { calls++ })

	dst := &ConsumerEvent{}
	dst.SetAck(src.TakeAck())

	src.Ack()
	assert.Equal(t, 0, calls)

	dst.Ack()
	assert.Equal(t, 1, calls)
}

func TestConsumerEvent_HandOverMovesSettlementAndClaim(t *testing.T) {
	acks, abandons := 0, 0
	ctx, cancel := context.WithCancel(context.Background())
	src := &ConsumerEvent{ClaimCtx: ctx}
	src.SetAck(func() { acks++ })
	src.SetAbandon(func() { abandons++ })

	dst := &ConsumerEvent{}
	src.HandOver(dst)
	src.Ack()
	src.Abandon()
	assert.Zero(t, acks+abandons, "the source no longer settles")

	assert.False(t, dst.ClaimEnded())
	cancel()
	assert.True(t, dst.ClaimEnded(), "the claim context travels with the callbacks")
	dst.Abandon()
	dst.Ack()
	assert.Equal(t, 1, abandons)
	assert.Equal(t, 1, acks)
}

func TestConsumerEvent_WithoutClaim(t *testing.T) {
	e := &ConsumerEvent{}
	assert.False(t, e.ClaimEnded())
	assert.Nil(t, e.ClaimDone())
	assert.NotPanics(t, e.Abandon)
}

func TestConsumerEvent_AckWithoutCallback(t *testing.T) {
	assert.NotPanics(t, func() { (&ConsumerEvent{}).Ack() })
}

func TestConsumerEventAckHandover(t *testing.T) {
	calls := 0
	e := &ConsumerEvent{}
	e.Ack()

	e.SetAck(func() { calls++ })
	fn := e.TakeAck()
	e.Ack()
	assert.Equal(t, 0, calls, "ack was handed over")

	fn()
	assert.Equal(t, 1, calls)
}

func TestHostConfigURIAndEventStatus(t *testing.T) {
	assert.Equal(t, "http://svc:8080/p", (&HostConfig{Name: "svc", Port: "8080", Path: "/p"}).URI())

	ev := &Event{}
	ev.SetFailure("r")
	assert.Equal(t, 1, ev.Failure)
	ev.SetSuccess("r")
	assert.Equal(t, 1, ev.Success)
	assert.Equal(t, 0, ev.Failure)
}
