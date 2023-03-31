package memorylanedb

import (
	"bytes"
	assert2 "github.com/stretchr/testify/assert"
	"math/rand"
	"testing"
	"time"
)

func TestEntry(t *testing.T) {
	assert := assert2.New(t)
	var buf bytes.Buffer
	codec := NewCodec(&buf)

	key := []byte("testKey")
	value := []byte("randomValue")
	entry := Entry{
		Checksum:  123456,
		Tstamp:    uint32(time.Now().Unix()),
		KeySize:   uint16(len(key)),
		ValueSize: uint32(len(value)),
		Key:       key,
		Value:     value,
	}

	t.Run("encodyEntry", func(t *testing.T) {
		_, err := codec.EncodeEntry(&entry)
		assert.NoError(err)
	})
	t.Run("decodyEntry", func(t *testing.T) {
		decodedEntry := Entry{}
		_, err := codec.DecodeEntry(&decodedEntry)
		assert.NoError(err)
		t.Logf("%+v", decodedEntry)
	})

}

func TestHint(t *testing.T) {
	assert := assert2.New(t)
	var buf bytes.Buffer
	codec := NewCodec(&buf)

	key := []byte("testKey")
	value := []byte("randomValue")
	hint := Hint{
		Tstamp:      uint32(time.Now().Unix()),
		KeySize:     uint16(len(key)),
		ValueSize:   uint32(len(value)),
		ValueOffset: uint32(rand.Int31()),
		Key:         key,
	}

	t.Run("encodeHint", func(t *testing.T) {
		_, err := codec.EncodeHint(&hint)
		assert.NoError(err)
	})
	t.Run("decodeHint", func(t *testing.T) {
		decodedHint := Hint{}
		err := codec.DecodeHint(&decodedHint)
		assert.NoError(err)
		t.Logf("%+v", decodedHint)
	})

}

func Benchmark_Encode(b *testing.B) {
	return
}
