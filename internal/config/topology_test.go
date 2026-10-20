package config

import (
	"strings"
	"testing"

	"bridge-kafka-service/internal/schemas"
)

func stanza(group, response string, topics ...string) *schemas.ConsumerConfig {
	return &schemas.ConsumerConfig{Topics: topics, Group: group, ResponseTopic: response}
}

func TestTopologyResponseCycles(t *testing.T) {
	cases := []struct {
		name      string
		consumers []*schemas.ConsumerConfig
		want      []string
	}{
		{"self loop", []*schemas.ConsumerConfig{stanza("g", "orders", "orders")},
			[]string{"orders -> orders", "consumer #1 (topics [orders])"}},
		{"loop through two stanzas", []*schemas.ConsumerConfig{stanza("g1", "b", "a"), stanza("g2", "a", "b")},
			[]string{"a -> b -> a", "consumer #1 (topics [a])", "consumer #2 (topics [b])"}},
		{"chain", []*schemas.ConsumerConfig{stanza("g1", "b", "a"), stanza("g2", "c", "b")}, nil},
		{"disabled stanza ignored", []*schemas.ConsumerConfig{stanza("g1", "a"), stanza("g2", "x", "a")}, nil},
		{"nil entry ignored", []*schemas.ConsumerConfig{nil, stanza("g", "out", "in")}, nil},
		{"no response topic", []*schemas.ConsumerConfig{stanza("g", "", "a")}, nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := Topology(&schemas.KafkaInfo{Consumers: c.consumers})
			if len(c.want) == 0 {
				if err != nil {
					t.Fatalf("valid topology rejected: %v", err)
				}
				return
			}
			for _, want := range c.want {
				if err == nil || !strings.Contains(err.Error(), want) {
					t.Fatalf("error %v must contain %q", err, want)
				}
			}
		})
	}
}

// A cycle is reported once, whichever topic the search reaches first, and output is stable.
func TestTopologyCycleReportedOnceAndStable(t *testing.T) {
	k := &schemas.KafkaInfo{Consumers: []*schemas.ConsumerConfig{
		stanza("g1", "c", "b"), stanza("g2", "a", "c"), stanza("g3", "b", "a"),
	}}

	_, first := Topology(k)
	_, second := Topology(k)

	if first == nil || strings.Count(first.Error(), "form a cycle") != 1 {
		t.Fatalf("want exactly one cycle, got %v", first)
	}
	if !strings.Contains(first.Error(), "a -> b -> c -> a") {
		t.Fatalf("the cycle starts at its smallest topic: %v", first)
	}
	if first.Error() != second.Error() {
		t.Fatalf("output must be stable:\n%v\n%v", first, second)
	}
}

func TestTopologySharedGroups(t *testing.T) {
	t.Run("shared group and topic", func(t *testing.T) {
		_, err := Topology(&schemas.KafkaInfo{Consumers: []*schemas.ConsumerConfig{
			stanza("bridge", "", "orders", "refunds"), stanza("bridge", "", "orders"),
		}})
		for _, want := range []string{"consumer #1", "consumer #2", `"bridge"`, "[orders]"} {
			if err == nil || !strings.Contains(err.Error(), want) {
				t.Fatalf("error %v must contain %q", err, want)
			}
		}
	})
	t.Run("shared group, disjoint topics", func(t *testing.T) {
		warnings, err := Topology(&schemas.KafkaInfo{Consumers: []*schemas.ConsumerConfig{
			stanza("bridge", "", "orders"), stanza("bridge", "", "payments"),
		}})
		if err != nil {
			t.Fatalf("disjoint topics are allowed: %v", err)
		}
		if len(warnings) != 1 || !strings.Contains(warnings[0], `"bridge"`) || !strings.Contains(warnings[0], "consumer #2") {
			t.Fatalf("want one warning naming both stanzas and the group, got %v", warnings)
		}
	})
	t.Run("fan-out across groups", func(t *testing.T) {
		warnings, err := Topology(&schemas.KafkaInfo{Consumers: []*schemas.ConsumerConfig{
			stanza("g1", "", "orders"), stanza("g2", "", "orders"),
		}})
		if err != nil || len(warnings) != 0 {
			t.Fatalf("fan-out is normal: %v %v", warnings, err)
		}
	})
	t.Run("disabled stanza ignored", func(t *testing.T) {
		warnings, err := Topology(&schemas.KafkaInfo{Consumers: []*schemas.ConsumerConfig{
			stanza("bridge", "", "orders"), stanza("bridge", ""),
		}})
		if err != nil || len(warnings) != 0 {
			t.Fatalf("a stanza without topics adds nothing: %v %v", warnings, err)
		}
	})
}
