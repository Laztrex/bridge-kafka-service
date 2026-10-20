package config

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	"bridge-kafka-service/internal/schemas"
)

// stanzaRef is an active consumer stanza and the name errors use for it.
type stanzaRef struct {
	label string
	conf  *schemas.ConsumerConfig
}

// Topology checks how the active consumer stanzas fit together: response topics must not lead back
// into their own input (a cycle), and two stanzas of one group must not share a topic. Two stanzas
// of one group with disjoint topics are a warning. Findings are sorted, so the output is stable.
func Topology(k *schemas.KafkaInfo) (warnings []string, err error) {
	var active []stanzaRef
	for i, c := range k.Consumers {
		if c != nil && len(c.Topics) > 0 {
			active = append(active, stanzaRef{label: fmt.Sprintf("consumer #%d (topics %v)", i+1, c.Topics), conf: c})
		}
	}
	errs := responseCycles(active)
	warnings, groupErrs := sharedGroups(active)
	return warnings, errors.Join(append(errs, groupErrs...)...)
}

// topicEdge leads from an input topic to the response topic of the stanza that reads it.
type topicEdge struct {
	to     string
	stanza string
}

// responseCycles finds every cycle in the graph whose edges lead from each input topic of a stanza
// to its response_topic. Each cycle is reported once, starting at its smallest topic.
func responseCycles(active []stanzaRef) []error {
	edges := map[string][]topicEdge{}
	for _, s := range active {
		if s.conf.ResponseTopic == "" {
			continue
		}
		for _, in := range s.conf.Topics {
			edges[in] = append(edges[in], topicEdge{to: s.conf.ResponseTopic, stanza: s.label})
		}
	}
	nodes := make([]string, 0, len(edges))
	for n := range edges {
		nodes = append(nodes, n)
	}
	slices.Sort(nodes)

	const (
		white = iota
		grey
		black
	)
	color := map[string]int{}
	var path []string
	var via []string
	seen := map[string]bool{}
	var errs []error

	var visit func(n string)
	visit = func(n string) {
		color[n] = grey
		path = append(path, n)
		for _, e := range edges[n] {
			switch color[e.to] {
			case white:
				via = append(via, e.stanza)
				visit(e.to)
				via = via[:len(via)-1]
			case grey:
				start := slices.Index(path, e.to)
				topics := slices.Clone(path[start:])
				stanzas := append(slices.Clone(via[start:]), e.stanza)
				if err := cycleError(topics, stanzas, seen); err != nil {
					errs = append(errs, err)
				}
			}
		}
		path = path[:len(path)-1]
		color[n] = black
	}
	for _, n := range nodes {
		if color[n] == white {
			visit(n)
		}
	}
	return errs
}

// cycleError describes the cycle topics[0] -> topics[1] -> ... -> topics[0], where stanzas[i] leads
// from topics[i] to the next, rotated to start at the smallest topic; nil if seen already holds it.
func cycleError(topics, stanzas []string, seen map[string]bool) error {
	first := slices.Index(topics, slices.Min(topics))
	topics = append(topics[first:], topics[:first]...)
	stanzas = append(stanzas[first:], stanzas[:first]...)
	key := strings.Join(topics, "\x00")
	if seen[key] {
		return nil
	}
	seen[key] = true
	return fmt.Errorf("response topics form a cycle %s -> %s through %s: every response would be consumed again as a request",
		strings.Join(topics, " -> "), topics[0], strings.Join(slices.Compact(stanzas), ", "))
}

// sharedGroups reports two active stanzas of one group: an error when they share a topic, whose
// partitions they would split between them, and a warning otherwise, since they rebalance together.
func sharedGroups(active []stanzaRef) (warnings []string, errs []error) {
	for i, a := range active {
		for _, b := range active[i+1:] {
			if a.conf.Group == "" || a.conf.Group != b.conf.Group {
				continue
			}
			var shared []string
			for _, t := range a.conf.Topics {
				if slices.Contains(b.conf.Topics, t) && !slices.Contains(shared, t) {
					shared = append(shared, t)
				}
			}
			slices.Sort(shared)
			if len(shared) > 0 {
				errs = append(errs, fmt.Errorf("%s and %s share group %q and topics %v: their partitions would be split between the two stanzas; use one stanza with a higher service.max_conn, or different groups",
					a.label, b.label, a.conf.Group, shared))
				continue
			}
			warnings = append(warnings, fmt.Sprintf("%s and %s share group %q: they rebalance together", a.label, b.label, a.conf.Group))
		}
	}
	return warnings, errs
}
