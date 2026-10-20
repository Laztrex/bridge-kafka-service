package codec

import (
	"encoding/json"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"bridge-kafka-service/internal/consts"
	"bridge-kafka-service/internal/schemas"
	"bridge-kafka-service/testutil/requiredheaders"
)

func consume(t *testing.T, value []byte, headers ...*sarama.RecordHeader) *schemas.RequestBody {
	t.Helper()
	body := &schemas.RequestBody{}
	GetConsumeMsg(&sarama.ConsumerMessage{Topic: "t", Value: value, Headers: headers, Timestamp: time.Now()}, body)
	return body
}

func TestGetConsumeMsg_PayloadCoercion(t *testing.T) {
	cases := []struct {
		name  string
		value []byte
		want  string
	}{
		{"tombstone becomes empty object", nil, `{}`},
		{"valid JSON verbatim", []byte(`{"a": 1}`), `{"a": 1}`},
		{"plain text becomes JSON string", []byte("hello world"), `"hello world"`},
		{"binary becomes base64 string", []byte{0xff, 0xfe, 0x00}, `"//4A"`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			assert.JSONEq(t, c.want, string(consume(t, c.value).Data))
		})
	}
}

func TestGetConsumeMsg_HeadersAreEnrichedAndBinarySafe(t *testing.T) {
	body := consume(t, []byte(`{}`),
		&sarama.RecordHeader{Key: []byte(consts.HeaderRequestIdKey), Value: []byte("rid-1")},
		&sarama.RecordHeader{Key: []byte("bin"), Value: []byte{0x01, 0x02}},
		&sarama.RecordHeader{Key: []byte("txt"), Value: []byte("héllo")},
	)

	assert.Equal(t, "rid-1", body.Headers[consts.HeaderRequestIdKey], "existing request id is reused")
	assert.Equal(t, "AQI=", body.Headers["bin"])
	assert.Equal(t, "héllo", body.Headers["txt"])
	assert.Contains(t, body.Headers, consts.HeaderClientId)
}

func TestGetConsumeMsg_GeneratesRequestIdWhenAbsent(t *testing.T) {
	body := consume(t, []byte(`{}`))
	assert.Len(t, body.Headers[consts.HeaderRequestIdKey], 36)
}

func TestGetConsumeMsg_TrackingHeadersIgnoreCase(t *testing.T) {
	body := &schemas.RequestBody{}

	requestId := GetConsumeMsg(&sarama.ConsumerMessage{Value: []byte(`{}`), Headers: []*sarama.RecordHeader{
		{Key: []byte("X-Request-Id"), Value: []byte("abc")},
		{Key: []byte("X-Client-Id"), Value: []byte("billing")},
		{Key: []byte("X-Trace-Id"), Value: []byte("t-1")},
	}}, body)

	assert.Equal(t, "abc", requestId, "the record's id is reused, not regenerated")
	assert.Equal(t, map[string]string{
		consts.HeaderRequestIdKey: "abc", consts.HeaderClientId: "billing", consts.HeaderTraceIdKey: "t-1",
	}, body.Headers, "stored under the lowercase wire names, with no default added beside them")
}

func TestGetConsumeMsg_LowercaseSpellingWins(t *testing.T) {
	body := consume(t, []byte(`{}`),
		&sarama.RecordHeader{Key: []byte("X-Request-Id"), Value: []byte("upper")},
		&sarama.RecordHeader{Key: []byte(consts.HeaderRequestIdKey), Value: []byte("lower")},
	)

	assert.Equal(t, "lower", body.Headers[consts.HeaderRequestIdKey])
	assert.NotContains(t, body.Headers, "X-Request-Id")
}

func TestGetConsumeMsg_RequiredHeadersGetNoDefault(t *testing.T) {
	requiredheaders.Set(t, consts.HeaderRequestIdKey, consts.HeaderClientId)
	body := &schemas.RequestBody{}

	requestId := GetConsumeMsg(&sarama.ConsumerMessage{Value: []byte(`{}`)}, body)

	assert.NotContains(t, body.Headers, consts.HeaderRequestIdKey, "a listed header stays absent")
	assert.NotContains(t, body.Headers, consts.HeaderClientId)
	assert.Len(t, requestId, 36, "logs and audit still get a request id")
}

func TestPrepareConsumeMsg_RequiredRequestIdAbsentStillNamesTheEvent(t *testing.T) {
	requiredheaders.Set(t, consts.HeaderRequestIdKey)
	ev := &schemas.Event{EventType: consts.EventConsume}

	out := PrepareConsumeMsg(&sarama.ConsumerMessage{Value: []byte(`{}`)}, ev, &schemas.RequestBody{})

	assert.Len(t, ev.RequestId, 36)
	assert.Equal(t, ev.RequestId, out.Values.RequestId)
	assert.NotContains(t, out.Values.Body.Headers, consts.HeaderRequestIdKey)
}

func TestPrepareConsumeMsg_SetsEventAndContext(t *testing.T) {
	ev := &schemas.Event{EventType: consts.EventConsume}
	body := &schemas.RequestBody{}
	out := PrepareConsumeMsg(&sarama.ConsumerMessage{Value: []byte(`{}`)}, ev, body)

	assert.Equal(t, 1, ev.Success)
	assert.Equal(t, ev.RequestId, out.Values.RequestId)
	assert.Same(t, body, out.Values.Body)
}

func TestPrepareProduceMsg(t *testing.T) {
	m := PrepareProduceMsg("topic", `{"a":1}`, map[string]string{consts.HeaderKafkaKey: "k", "h": "v"})

	key, _ := m.Key.Encode()
	val, _ := m.Value.Encode()
	assert.Equal(t, "topic", m.Topic)
	assert.Equal(t, "k", string(key))
	assert.Equal(t, `{"a":1}`, string(val))
	assert.Len(t, m.Headers, 2)
}

func TestIsBinaryData(t *testing.T) {
	assert.False(t, IsBinaryData("text with\ttabs and ünïcödé"))
	assert.True(t, IsBinaryData("a\x00b"))
}

// bs is a literal backslash; escape sequences are assembled from it so the test
// inputs cannot be altered by any tool that processes "\u"/"\x" in source text.
const bs = "\\"

// Coercion is total: every byte sequence yields valid JSON (see spec "Payload
// coercion never fails"), and each documented rule keeps its exact output.
func TestCoercionRules_ExactOutputs(t *testing.T) {
	cases := []struct {
		name  string
		value []byte
		want  string
	}{
		{"nil -> empty object", nil, `{}`},
		{"empty -> empty object", []byte{}, `{}`},
		{"valid JSON verbatim (spacing kept)", []byte(`{ "a" :  1 }`), `{ "a" :  1 }`},
		{"valid JSON scalar", []byte(`42`), `42`},
		{"valid JSON with escaped backslash is untouched", []byte(`{"a":"` + bs + bs + `u0041"}`), `{"a":"` + bs + bs + `u0041"}`},
		{"unicode-escaped structure recovered", []byte("{" + bs + "u0022a" + bs + "u0022:1}"), `{"a":1}`},
		{"hex-escaped structure recovered", []byte("{" + bs + "x22a" + bs + "x22:1}"), `{"a":1}`},
		{"escapes in non-JSON text stay literal", []byte("not json " + bs + "u0041"), `"not json ` + bs + bs + `u0041"`},
		{"plain UTF-8 text", []byte("héllo wörld"), `"héllo wörld"`},
		{"text with quotes is escaped", []byte(`say "hi"`), `"say ` + bs + `"hi` + bs + `""`},
		{"invalid UTF-8 -> base64", []byte{0xff, 0xfe}, `"//4="`},
		{"truncated JSON is text", []byte(`[1,`), `"[1,"`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			assert.Equal(t, c.want, string(consume(t, c.value).Data))
		})
	}
}

func TestCoercionNeverFails(t *testing.T) {
	r := rand.New(rand.NewSource(1))
	inputs := [][]byte{
		[]byte(bs), []byte(bs + "u"), []byte(bs + "uZZZZ"), []byte(bs + "x"), []byte(`"`), []byte(`{`), []byte(`[1,`),
		[]byte("\xef\xbf\xbd"), []byte("\xc3\x28"), []byte("a\x00b"), []byte(strings.Repeat(bs+"u00", 50)),
	}
	for i := 0; i < 500; i++ {
		b := make([]byte, r.Intn(64))
		r.Read(b)
		inputs = append(inputs, b)
	}
	for _, in := range inputs {
		body := consume(t, in)
		require.True(t, json.Valid(body.Data), "input %q produced invalid JSON %q", in, body.Data)
	}
}

func TestPrepareProduceMsg_PartitionKeyLookupIgnoresCase(t *testing.T) {
	for _, name := range []string{consts.HeaderKafkaKey, "X-Kafka-Key"} {
		m := PrepareProduceMsg("t", `{}`, map[string]string{name: "user-42"})
		key, _ := m.Key.Encode()
		assert.Equal(t, "user-42", string(key), "header %q", name)
	}
	m := PrepareProduceMsg("t", `{}`, map[string]string{"other": "v"})
	key, _ := m.Key.Encode()
	assert.Empty(t, key)
}

func TestUnescapeUnicodeCharactersInJSON(t *testing.T) {
	got, err := UnescapeUnicodeCharactersInJSON([]byte("x " + bs + "u0041"))
	if err != nil || string(got) != "x A" {
		t.Fatalf("unicode escape not decoded: %q, %v", got, err)
	}
}

func TestUnescapeUnicodeCharactersInJSON_HexAndErrorPaths(t *testing.T) {
	got, err := UnescapeUnicodeCharactersInJSON([]byte("a " + bs + "x41"))
	if err != nil || string(got) != "a A" {
		t.Fatalf("hex escape: %q, %v", got, err)
	}

	if _, err := UnescapeUnicodeCharactersInJSON([]byte("bad " + bs + "uZZ")); err == nil {
		t.Fatal("an invalid escape must be reported")
	}

	empty, err := UnescapeUnicodeCharactersInJSON(nil)
	if err != nil || len(empty) != 0 {
		t.Fatalf("empty input: %q, %v", empty, err)
	}
}
