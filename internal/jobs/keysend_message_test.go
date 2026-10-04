package jobs

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestKeysendMessage(t *testing.T) {
	assert.Equal(t, "hello", keysendMessage([]byte("hello")))
	assert.Equal(t, "ab", keysendMessage([]byte{'a', 0xff, 'b'}), "invalid UTF-8 dropped like Python errors='ignore'")
	assert.Equal(t, "ab", keysendMessage([]byte{'a', 0x00, 'b'}), "NUL dropped, Postgres rejects it")
	assert.Equal(t, "äö", keysendMessage([]byte("äö")), "valid multi-byte kept")
	long := strings.Repeat("ä", 1200)
	assert.Equal(t, strings.Repeat("ä", 1000), keysendMessage([]byte(long)), "capped at 1000 runes")
}
