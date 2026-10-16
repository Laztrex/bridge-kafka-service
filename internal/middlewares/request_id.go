package middlewares

import (
	"bridge-kafka-service/internal/consts"
	"bridge-kafka-service/internal/schemas"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/rs/zerolog/log"
)

// NewContextWithRequestID stores the request's publish parameters in ctx under key.
func NewContextWithRequestID(
	ctx context.Context,
	body *schemas.RequestBody,
	key consts.ContextKeys,
	requestId string,
	produceTopic string,
	path string,
) context.Context {

	val := schemas.ContextValues{
		RequestId: requestId, Body: body, Topic: produceTopic, Path: path,
	}

	return context.WithValue(ctx, key, val)
}

// RequestIdMiddleware validates the publish request (tracking headers, body) and passes it to next via
// the context. Tracking headers are echoed on every answer, errors included.
func RequestIdMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		processedHeaders, err := ProcessHeaders(r.Header)
		requestId := processedHeaders[consts.HeaderRequestIdKey]
		if err != nil {
			msg := fmt.Sprintf("Error processing headers: %v", err)
			errCheckRequest(w, requestId, msg, http.StatusUnprocessableEntity)
			return
		}

		setHttpHeaders(w, processedHeaders)

		produceTopicName := r.Header.Get(consts.HeaderProduceTopicName)
		if produceTopicName == "" {
			msg := fmt.Sprintf("RequestHeader not contains header '%s'", consts.HeaderProduceTopicName)
			errCheckRequest(w, requestId, msg, http.StatusUnprocessableEntity)
			return
		}

		body := &schemas.RequestBody{
			Headers: make(map[string]string),
		}

		setKafkaHeaders(r.Header, body)

		err = getBodyRequest(w, r, body, requestId)
		if err != nil {
			return
		}

		ctx = NewContextWithRequestID(
			ctx, body, consts.PublishContextKey, requestId, produceTopicName, r.URL.Path,
		)

		log.Info().Str(consts.HeaderRequestIdKey, requestId).Msg("request started...")

		next.ServeHTTP(w, r.WithContext(ctx))

		log.Info().Str(consts.HeaderRequestIdKey, requestId).Msg("request finished!")
	})
}

// getBodyRequest checks Content-Type and decodes the JSON request body into body.Data.
func getBodyRequest(w http.ResponseWriter, r *http.Request, body *schemas.RequestBody, requestId string) error {

	ct := r.Header.Get("Content-Type")
	if ct != "" {
		mediaType := strings.ToLower(strings.TrimSpace(strings.Split(ct, ";")[0]))
		if mediaType != "application/json" {
			msg := "Content-Type header is not application/json"
			http.Error(w, msg, http.StatusUnsupportedMediaType)
			return errors.New(msg)
		}
	}

	r.Body = http.MaxBytesReader(w, r.Body, 2097152)

	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()

	err := dec.Decode(&body.Data)
	if err != nil {
		var syntaxError *json.SyntaxError
		var unmarshalTypeError *json.UnmarshalTypeError

		switch {
		case errors.As(err, &syntaxError):
			msg := fmt.Sprintf("RequestBody body contains badly-formed JSON (at position %d)", syntaxError.Offset)
			errCheckRequest(w, requestId, msg, http.StatusUnprocessableEntity)
		case errors.Is(err, io.ErrUnexpectedEOF):
			msg := "RequestBody body contains badly-formed JSON"
			errCheckRequest(w, requestId, msg, http.StatusUnprocessableEntity)
		case errors.As(err, &unmarshalTypeError):
			msg := fmt.Sprintf("RequestBody body contains an invalid value for the %q field (at position %d)", unmarshalTypeError.Field, unmarshalTypeError.Offset)
			errCheckRequest(w, requestId, msg, http.StatusUnprocessableEntity)
		case strings.HasPrefix(err.Error(), "json: unknown field "):
			fieldName := strings.TrimPrefix(err.Error(), "json: unknown field ")
			msg := fmt.Sprintf("RequestBody body contains unknown field %s", fieldName)
			errCheckRequest(w, requestId, msg, http.StatusUnprocessableEntity)
		case errors.Is(err, io.EOF):
			msg := "RequestBody body must not be empty"
			errCheckRequest(w, requestId, msg, http.StatusUnprocessableEntity)
		case isBodyTooLarge(err):
			msg := "RequestBody body must not be larger than 2MB"
			errCheckRequest(w, requestId, msg, http.StatusUnprocessableEntity)
		default:
			errCheckRequest(w, requestId, err.Error(), http.StatusInternalServerError)
		}
		return err
	}

	if err = dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		msg := "RequestBody body must only contain a single JSON object"
		http.Error(w, msg, http.StatusUnprocessableEntity)
		return errors.New(msg)
	}

	return nil
}

// errCheckRequest logs a rejected request and writes the JSON error reply.
func errCheckRequest(w http.ResponseWriter, requestId string, msg string, statusCode int) {
	log.Info().Str(consts.HeaderRequestIdKey, requestId).Msgf("not found: %s", msg)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)

	errAns := schemas.ResponseBody{
		Result: map[string]string{},
		Status: "failure",
		Detail: msg,
	}
	err := json.NewEncoder(w).Encode(errAns)
	if err != nil {
		log.Err(err).Str(consts.HeaderRequestIdKey, requestId).Msgf("unexpected error: %v", err.Error())
	}
}

func isBodyTooLarge(err error) bool {
	var maxErr *http.MaxBytesError
	return errors.As(err, &maxErr)
}
