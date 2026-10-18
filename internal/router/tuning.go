package router

import (
	"fmt"
	"os"
	"reflect"
	"strings"
	"time"

	"github.com/IBM/sarama"
	"github.com/rs/zerolog/log"
	"gopkg.in/yaml.v3"
)

// Tuning is a Kafka client tuning file loaded and checked once at startup: the YAML tree
// with the application's own keys, the rebalance strategy and keys that cannot be set from
// YAML removed, plus the values of those application keys. A nil *Tuning applies nothing.
type Tuning struct {
	file               string
	node               *yaml.Node
	strategy           sarama.BalanceStrategy
	autoCommitInterval time.Duration
}

// rebalanceStrategies are the values accepted for consumer.group.rebalance.strategy.
var rebalanceStrategies = []struct {
	name string
	new  func() sarama.BalanceStrategy
}{
	{"range", sarama.NewBalanceStrategyRange},
	{"roundrobin", sarama.NewBalanceStrategyRoundRobin},
	{"sticky", sarama.NewBalanceStrategySticky},
	{"cooperative-sticky", sarama.NewBalanceStrategyCooperativeSticky},
}

// LoadConsumerTuning loads the consumer tuning file: besides sarama settings it accepts
// OffsetsAutoCommitInterval and consumer.group.rebalance.strategy (range, roundrobin, sticky,
// cooperative-sticky). A missing or unparsable file and an unknown strategy are errors;
// unknown keys and keys that cannot be set from YAML are logged as warnings and ignored.
func LoadConsumerTuning() (*Tuning, error) {
	t, root, err := readTuning(consumerConfigPath)
	if err != nil || root == nil {
		return t, err
	}

	if v := take(root, "OffsetsAutoCommitInterval"); v != nil {
		if err := v.Decode(&t.autoCommitInterval); err != nil {
			return nil, fmt.Errorf("%s: OffsetsAutoCommitInterval: %w", t.file, err)
		}
	}
	if v := take(root, "consumer", "group", "rebalance", "strategy"); v != nil {
		if t.strategy, err = parseStrategy(t.file, v); err != nil {
			return nil, err
		}
	}
	return t, t.check(root)
}

// LoadProducerTuning loads the producer tuning file with the same rules as
// LoadConsumerTuning, without the consumer-only keys.
func LoadProducerTuning() (*Tuning, error) {
	t, root, err := readTuning(producerConfigPath)
	if err != nil || root == nil {
		return t, err
	}
	return t, t.check(root)
}

// readTuning reads and parses a tuning file; root is nil for an empty file.
func readTuning(path string) (*Tuning, *yaml.Node, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, nil, fmt.Errorf("unable to locate configuration file %s: %w", path, err)
	}
	t := &Tuning{file: path}
	var doc yaml.Node
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		return nil, nil, fmt.Errorf("unable to parse configuration file %s: %w", path, err)
	}
	if len(doc.Content) == 0 {
		return t, nil, nil
	}
	root := doc.Content[0]
	if root.Kind != yaml.MappingNode {
		return nil, nil, fmt.Errorf("unable to parse configuration file %s: the top level must be a mapping", path)
	}
	t.node = &doc
	return t, root, nil
}

// check prunes root against sarama.Config, warning about every key it removes, and decodes
// the result once into a fresh config so value errors surface at startup.
func (t *Tuning) check(root *yaml.Node) error {
	prune(t.file, root, reflect.TypeOf(sarama.Config{}), "")
	return t.apply(sarama.NewConfig())
}

// apply overlays the tuning on cfg: the sarama settings, then the rebalance strategy and the
// auto-commit interval when they were set.
func (t *Tuning) apply(cfg *sarama.Config) (err error) {
	if t == nil {
		return nil
	}
	if t.node != nil {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("unable to apply configuration file %s: %v", t.file, r)
			}
		}()
		if err := t.node.Decode(cfg); err != nil {
			return fmt.Errorf("unable to parse configuration file %s: %w", t.file, err)
		}
	}
	if t.strategy != nil {
		cfg.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{t.strategy}
	}
	if t.autoCommitInterval > 0 {
		cfg.Consumer.Offsets.AutoCommit.Interval = t.autoCommitInterval
	}
	return nil
}

func parseStrategy(file string, v *yaml.Node) (sarama.BalanceStrategy, error) {
	names := make([]string, len(rebalanceStrategies))
	for i, s := range rebalanceStrategies {
		names[i] = s.name
		if v.Kind == yaml.ScalarNode && strings.EqualFold(strings.TrimSpace(v.Value), s.name) {
			return s.new(), nil
		}
	}
	return nil, fmt.Errorf("%s: consumer.group.rebalance.strategy %q is not supported (allowed: %s)",
		file, v.Value, strings.Join(names, ", "))
}

// take removes the value at path from the mapping root and returns it, or nil when absent.
func take(root *yaml.Node, path ...string) *yaml.Node {
	m := root
	for i, key := range path {
		if m.Kind != yaml.MappingNode {
			return nil
		}
		idx := -1
		for j := 0; j+1 < len(m.Content); j += 2 {
			if m.Content[j].Value == key {
				idx = j
				break
			}
		}
		if idx < 0 {
			return nil
		}
		if i == len(path)-1 {
			v := m.Content[idx+1]
			m.Content = append(m.Content[:idx], m.Content[idx+2:]...)
			return v
		}
		m = m.Content[idx+1]
	}
	return nil
}

// prune walks a mapping against the struct type t with the yaml.v3 naming rule (lowercased
// field name) and removes, with a warning, every key that matches no field and every key whose
// field cannot be decoded from YAML (interface, func or channel values), which would otherwise
// be dropped silently or crash the decoder.
func prune(file string, m *yaml.Node, t reflect.Type, prefix string) {
	kept := m.Content[:0]
	for i := 0; i+1 < len(m.Content); i += 2 {
		key, val := m.Content[i], m.Content[i+1]
		path := prefix + key.Value
		f, ok := fieldByYAMLName(t, key.Value)
		if !ok {
			log.Warn().Msgf("unknown tuning key %s in %s is ignored", path, file)
			continue
		}
		ft := f.Type
		for ft.Kind() == reflect.Pointer {
			ft = ft.Elem()
		}
		if !settable(ft) {
			log.Warn().Msgf("tuning key %s in %s cannot be set from YAML and is ignored", path, file)
			continue
		}
		if ft.Kind() == reflect.Struct && ft != reflect.TypeOf(time.Time{}) && val.Kind == yaml.MappingNode {
			prune(file, val, ft, path+".")
		}
		kept = append(kept, key, val)
	}
	m.Content = kept
}

func fieldByYAMLName(t reflect.Type, name string) (reflect.StructField, bool) {
	for i := range t.NumField() {
		f := t.Field(i)
		if !f.IsExported() {
			continue
		}
		yamlName := strings.ToLower(f.Name)
		if tag, ok := f.Tag.Lookup("yaml"); ok {
			tagName, _, _ := strings.Cut(tag, ",")
			if tagName == "-" {
				continue
			}
			if tagName != "" {
				yamlName = tagName
			}
		}
		if yamlName == name {
			return f, true
		}
	}
	return reflect.StructField{}, false
}

// settable reports whether yaml.v3 can decode a YAML value into a field of type t.
func settable(t reflect.Type) bool {
	switch t.Kind() {
	case reflect.Interface, reflect.Func, reflect.Chan, reflect.UnsafePointer:
		return false
	case reflect.Slice, reflect.Array:
		return settable(t.Elem())
	case reflect.Map:
		return settable(t.Key()) && settable(t.Elem())
	}
	return true
}
