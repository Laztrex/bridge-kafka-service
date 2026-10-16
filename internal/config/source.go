package config

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/go-viper/mapstructure/v2"
	"go.yaml.in/yaml/v3"
)

// source looks keys up exactly as the former viper-based loader did, so existing config files and
// environment overrides keep working:
//   - keys are case-insensitive (logLevel and loglevel are the same key);
//   - a nested key may be written nested (http: {addr: ...}) or dotted (http.addr: ...);
//   - a non-empty environment variable named after the key (upper case, dots replaced by
//     underscores, e.g. HTTP_ADDR) overrides a scalar key; empty variables are ignored.
type source struct {
	values map[string]any
	getenv func(string) string
}

func newSource(raw []byte, getenv func(string) string) (*source, error) {
	var values map[string]any
	if err := yaml.Unmarshal(raw, &values); err != nil {
		return nil, err
	}
	if values == nil {
		values = map[string]any{}
	}
	return &source{values: lowercaseKeys(values).(map[string]any), getenv: getenv}, nil
}

// lowercaseKeys lower-cases every map key, recursively through maps and lists.
func lowercaseKeys(v any) any {
	switch t := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, val := range t {
			out[strings.ToLower(k)] = lowercaseKeys(val)
		}
		return out
	case map[any]any:
		out := make(map[string]any, len(t))
		for k, val := range t {
			out[strings.ToLower(fmt.Sprint(k))] = lowercaseKeys(val)
		}
		return out
	case []any:
		for i, val := range t {
			t[i] = lowercaseKeys(val)
		}
		return t
	default:
		return v
	}
}

// find returns the file value of key. The longest dotted prefix that exists as a literal key wins,
// so "kafka.tls.ca.path: x" at the top level and the nested form are both found.
func (s *source) find(key string) (any, bool) {
	return findPath(s.values, strings.Split(strings.ToLower(key), "."))
}

func findPath(m map[string]any, path []string) (any, bool) {
	for i := len(path); i > 0; i-- {
		v, ok := m[strings.Join(path[:i], ".")]
		if !ok {
			continue
		}
		if i == len(path) {
			return v, true
		}
		if sub, isMap := v.(map[string]any); isMap {
			if found, ok := findPath(sub, path[i:]); ok {
				return found, true
			}
		}
	}
	return nil, false
}

// envName is the environment variable that overrides key.
func envName(key string) string {
	return strings.ToUpper(strings.ReplaceAll(key, ".", "_"))
}

// value returns the environment override of key, otherwise its file value.
func (s *source) value(key string) (any, bool) {
	if v := s.getenv(envName(key)); v != "" {
		return v, true
	}
	return s.find(key)
}

func (s *source) String(key string) string {
	v, _ := s.value(key)
	return toString(v)
}

func (s *source) Bool(key string) bool {
	v, _ := s.value(key)
	switch t := v.(type) {
	case bool:
		return t
	case string:
		b, _ := strconv.ParseBool(t)
		return b
	case int:
		return t != 0
	case float64:
		return t != 0
	default:
		return false
	}
}

func toString(v any) string {
	switch t := v.(type) {
	case nil:
		return ""
	case string:
		return t
	case bool:
		return strconv.FormatBool(t)
	case int:
		return strconv.Itoa(t)
	case int64:
		return strconv.FormatInt(t, 10)
	case uint64:
		return strconv.FormatUint(t, 10)
	case float64:
		return strconv.FormatFloat(t, 'f', -1, 64)
	default:
		return fmt.Sprint(t)
	}
}

// decode decodes the file value of key into target with the former loader's rules: weak typing
// (port: 8080 becomes "8080"), "a,b" strings split into lists, "5s" strings parsed as durations.
// It returns the keys inside the section that target does not know.
func (s *source) decode(key string, target any) ([]string, error) {
	v, ok := s.find(key)
	if !ok || v == nil {
		return nil, nil
	}
	var md mapstructure.Metadata
	dec, err := mapstructure.NewDecoder(&mapstructure.DecoderConfig{
		Metadata:         &md,
		WeaklyTypedInput: true,
		DecodeHook: mapstructure.ComposeDecodeHookFunc(
			numberToSeconds(),
			mapstructure.StringToTimeDurationHookFunc(),
			stringToWeakSlice(","),
		),
		Result: target,
	})
	if err != nil {
		return nil, err
	}
	if err := dec.Decode(v); err != nil {
		return nil, fmt.Errorf("%s: %w", key, err)
	}
	unused := make([]string, 0, len(md.Unused))
	for _, u := range md.Unused {
		if strings.HasPrefix(u, "[") {
			unused = append(unused, key+u)
		} else {
			unused = append(unused, key+"."+u)
		}
	}
	return unused, nil
}

// stringToWeakSlice splits a string into a list ("" becomes an empty list), as viper did.
func stringToWeakSlice(sep string) mapstructure.DecodeHookFunc {
	return func(f reflect.Type, t reflect.Type, data any) (any, error) {
		if f.Kind() != reflect.String || t.Kind() != reflect.Slice {
			return data, nil
		}
		raw := data.(string)
		if raw == "" {
			return []string{}, nil
		}
		return strings.Split(raw, sep), nil
	}
}

// numberToSeconds reads a number given for a duration as seconds (timeout: 10 is ten seconds), as
// the config format has always documented; strings like "10s" are parsed as Go durations.
func numberToSeconds() mapstructure.DecodeHookFunc {
	durationType := reflect.TypeOf(time.Duration(0))
	return func(f reflect.Type, t reflect.Type, data any) (any, error) {
		if t != durationType {
			return data, nil
		}
		switch v := data.(type) {
		case int:
			return time.Duration(v) * time.Second, nil
		case int64:
			return time.Duration(v) * time.Second, nil
		case uint64:
			return time.Duration(v) * time.Second, nil
		case float64:
			return time.Duration(v * float64(time.Second)), nil
		}
		return data, nil
	}
}
