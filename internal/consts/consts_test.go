package consts_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"bridge-kafka-service/internal/consts"
	"bridge-kafka-service/testutil/requiredheaders"
)

func TestRequiredHeaders_EmptyByDefault(t *testing.T) {
	assert.Empty(t, consts.RequiredHeaders)
	assert.False(t, consts.IsRequiredHeader(consts.HeaderRequestIdKey))
}

func TestIsRequiredHeader_IgnoresCase(t *testing.T) {
	requiredheaders.Set(t, consts.HeaderClientId)

	assert.True(t, consts.IsRequiredHeader("x-client-id"))
	assert.True(t, consts.IsRequiredHeader("X-Client-Id"))
	assert.False(t, consts.IsRequiredHeader(consts.HeaderRequestIdKey))
}
