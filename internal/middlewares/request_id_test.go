package middlewares

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"bridge-kafka-service/internal/consts"
	"bridge-kafka-service/internal/schemas"
	"bridge-kafka-service/testutil/requiredheaders"
)

func serve(body, contentType string) (*httptest.ResponseRecorder, bool) {
	called := false
	h := RequestIdMiddleware(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { called = true }))
	r := httptest.NewRequest(http.MethodPost, "/api/v1/publish", strings.NewReader(body))
	r.Header.Set(consts.HeaderProduceTopicName, "t")
	if contentType != "" {
		r.Header.Set("Content-Type", contentType)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w, called
}

func TestRequestIdMiddleware_Validation(t *testing.T) {
	cases := []struct {
		name, body, ct string
		status         int
		passes         bool
	}{
		{"valid", `{"a":1}`, "application/json", 200, true},
		{"wrong content type", `{"a":1}`, "text/plain", 415, false},
		{"two objects", `{"a":1}{"b":2}`, "application/json", 422, false},
		{"empty", ``, "application/json", 422, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			w, called := serve(c.body, c.ct)
			if w.Code != c.status || called != c.passes {
				t.Fatalf("status=%d called=%v, want %d/%v", w.Code, called, c.status, c.passes)
			}
		})
	}
}

type failingBody struct{}

func (failingBody) Read([]byte) (int, error) { return 0, errors.New("read failed") }

func TestRequestIdMiddleware_BodyErrorResponses(t *testing.T) {
	cases := []struct {
		name   string
		body   io.Reader
		status int
		detail string
	}{
		{"truncated JSON", strings.NewReader(`{"a":`), 422, "badly-formed"},
		{"syntax error", strings.NewReader(`{"a" 1}`), 422, "badly-formed JSON (at position"},
		{"too large", strings.NewReader(`{"a":"` + strings.Repeat("x", 2*1024*1024+16) + `"}`), 422, "larger than 2MB"},
		{"unreadable body", failingBody{}, 500, "read failed"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			called := false
			h := RequestIdMiddleware(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { called = true }))
			r := httptest.NewRequest(http.MethodPost, "/p", c.body)
			r.Header.Set(consts.HeaderProduceTopicName, "t")
			w := httptest.NewRecorder()

			h.ServeHTTP(w, r)

			if called || w.Code != c.status || !strings.Contains(w.Body.String(), c.detail) {
				t.Fatalf("code=%d called=%v body=%s", w.Code, called, w.Body.String())
			}
		})
	}
}

func TestRequestIdMiddleware_MissingTopicHeader(t *testing.T) {
	h := RequestIdMiddleware(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/p", strings.NewReader(`{}`)))
	if w.Code != 422 || !strings.Contains(w.Body.String(), consts.HeaderProduceTopicName) {
		t.Fatalf("code=%d body=%s", w.Code, w.Body.String())
	}
}

func TestIsBodyTooLarge(t *testing.T) {
	if !isBodyTooLarge(&http.MaxBytesError{Limit: 1}) || isBodyTooLarge(errors.New("x")) {
		t.Fatal("isBodyTooLarge must match only *http.MaxBytesError")
	}
}

func TestRequestIdMiddleware_ErrorAnswersAreJSONAndCarryTheRequestId(t *testing.T) {
	h := RequestIdMiddleware(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	r := httptest.NewRequest(http.MethodPost, "/p", strings.NewReader(`{}`))
	r.Header.Set(consts.HeaderRequestIdKey, "client-rid")
	w := httptest.NewRecorder()

	h.ServeHTTP(w, r)

	if w.Code != 422 {
		t.Fatalf("code=%d", w.Code)
	}
	if got := w.Header().Get("Content-Type"); got != "application/json" {
		t.Fatalf("Content-Type=%q", got)
	}
	if got := w.Header().Get(consts.HeaderRequestIdKey); got != "client-rid" {
		t.Fatalf("request id not echoed on the error answer: %q", got)
	}
}

// unreadBody fails the test if the middleware reads the request body.
type unreadBody struct{ t *testing.T }

func (b unreadBody) Read([]byte) (int, error) {
	b.t.Error("the body must not be read")
	return 0, nil
}

// publish runs one publish request through the middleware and returns the answer and, when the
// handler was reached, the values it received.
func publish(r *http.Request) (*httptest.ResponseRecorder, *schemas.ContextValues) {
	var got *schemas.ContextValues
	h := RequestIdMiddleware(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		v := r.Context().Value(consts.PublishContextKey).(schemas.ContextValues)
		got = &v
	}))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w, got
}

func publishRequest(body string) *http.Request {
	r := httptest.NewRequest(http.MethodPost, "/api/v1/publish", strings.NewReader(body))
	r.Header.Set(consts.HeaderProduceTopicName, "t")
	r.Header.Set("Content-Type", "application/json")
	return r
}

func TestRequiredHeaders_EmptyListKeepsPublishUnchanged(t *testing.T) {
	w, got := publish(publishRequest(`{"a":1}`))

	require.NotNil(t, got, "the request reaches the handler")
	assert.Equal(t, http.StatusOK, w.Code)
	assert.NotEmpty(t, got.Body.Headers[consts.HeaderRequestIdKey], "defaults still apply")
}

func TestRequiredHeaders_MissingOnPublishIsRejected(t *testing.T) {
	requiredheaders.Set(t, consts.HeaderClientId)
	r := httptest.NewRequest(http.MethodPost, "/api/v1/publish", unreadBody{t})
	r.Header.Set(consts.HeaderProduceTopicName, "t")
	r.Header.Set("Content-Type", "application/json")

	w, got := publish(r)

	assert.Nil(t, got, "nothing is published")
	assert.Equal(t, http.StatusUnprocessableEntity, w.Code)
	var answer schemas.ResponseBody
	require.NoError(t, json.NewDecoder(w.Body).Decode(&answer))
	assert.Equal(t, "failure", answer.Status)
	assert.Contains(t, answer.Detail, consts.HeaderClientId)
	assert.NotEmpty(t, w.Header().Get(consts.HeaderRequestIdKey), "the request id is echoed on the error")
}

func TestRequiredHeaders_PresentOnPublishIsCarriedOntoTheRecord(t *testing.T) {
	requiredheaders.Set(t, consts.HeaderClientId)
	r := publishRequest(`{"a":1}`)
	r.Header.Set("X-Client-Id", "billing")

	w, got := publish(r)

	require.NotNil(t, got)
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "billing", got.Body.Headers[consts.HeaderClientId], "carried under the lowercase wire name")
}
