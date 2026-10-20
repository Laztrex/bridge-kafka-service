package transport

import (
	"fmt"
	"maps"
	"net/http"
	"strings"
	"unicode/utf8"

	"github.com/rs/zerolog/log"
	"golang.org/x/net/http/httpguts"

	"bridge-kafka-service/internal/consts"
)

// headerIssue is a record header the transport cannot carry. With drop it is left out of the
// call; without it the transport keeps the header where it can (gRPC keeps x-request-id in details
// and only skips the metadata copy).
type headerIssue struct {
	name string
	drop bool
}

// headerChecker is implemented by a transport that cannot carry every header value.
type headerChecker interface {
	headerIssues(headers map[string]string) []headerIssue
}

// transportControlledHeaders belong to the HTTP connection or message framing, so an HTTP call
// never takes them from a record: the hop-by-hop headers of net/http/httputil.ReverseProxy, plus
// Host, Content-Length and Content-Type.
var transportControlledHeaders = []string{
	"Connection", "Proxy-Connection", "Keep-Alive", "Proxy-Authenticate", "Proxy-Authorization",
	"Te", "Trailer", "Transfer-Encoding", "Upgrade", "Host", "Content-Length", "Content-Type",
}

func isTransportControlled(name string) bool {
	for _, h := range transportControlledHeaders {
		if strings.EqualFold(h, name) {
			return true
		}
	}
	return false
}

// applyHeaderPolicy decides which record headers go into the upstream call. A required header
// (consts.RequiredHeaders) that is absent, or that the transport cannot carry, is a permanent
// failure; any other header the transport cannot carry is reported with a warning and, when the
// issue says so, left out. headers itself is never modified.
func applyHeaderPolicy(upstream Upstream, headers map[string]string, requestId string) (map[string]string, error) {
	for _, name := range consts.RequiredHeaders {
		if !hasHeader(headers, name) {
			return nil, permanentError{fmt.Errorf("required header %s is missing", name)}
		}
	}

	checker, ok := upstream.(headerChecker)
	if !ok {
		return headers, nil
	}
	out, cloned := headers, false
	for _, issue := range checker.headerIssues(headers) {
		if consts.IsRequiredHeader(issue.name) {
			return nil, permanentError{fmt.Errorf("required header %s cannot be carried by the transport", issue.name)}
		}
		log.Warn().Str(consts.HeaderRequestIdKey, requestId).Str("header", issue.name).
			Msg("header cannot be carried by the upstream transport, leaving it out")
		if issue.drop {
			if !cloned {
				out, cloned = maps.Clone(headers), true
			}
			delete(out, issue.name)
		}
	}
	return out, nil
}

func hasHeader(headers map[string]string, name string) bool {
	for k := range headers {
		if strings.EqualFold(k, name) {
			return true
		}
	}
	return false
}

// headerIssues reports the headers HTTP cannot carry: an invalid field name, or a value with a
// control character other than tab. Transport-controlled headers are left to Call.
func (u *httpUpstream) headerIssues(headers map[string]string) []headerIssue {
	var out []headerIssue
	for k, v := range headers {
		if isTransportControlled(k) {
			continue
		}
		if !httpguts.ValidHeaderFieldName(k) || !httpguts.ValidHeaderFieldValue(v) {
			out = append(out, headerIssue{name: k, drop: true})
		}
	}
	return out
}

// headerIssues reports the headers gRPC cannot carry: text that is not valid UTF-8 cannot go into
// details, and an x-request-id that is not printable ASCII cannot go into the metadata copy.
func (u *grpcUpstream) headerIssues(headers map[string]string) []headerIssue {
	var out []headerIssue
	for k, v := range headers {
		switch {
		case !utf8.ValidString(k) || !utf8.ValidString(v):
			out = append(out, headerIssue{name: k, drop: true})
		case k == consts.HeaderRequestIdKey && !printableASCII(v):
			out = append(out, headerIssue{name: k, drop: false})
		}
	}
	return out
}

// setRecordHeaders copies the record headers onto an HTTP request, except the transport-controlled
// ones, and sets the JSON content type the normalised payload always has.
func setRecordHeaders(h http.Header, headers map[string]string) {
	for k, v := range headers {
		if !isTransportControlled(k) {
			h.Set(k, v)
		}
	}
	h.Set("Content-Type", "application/json")
}
