package memorylanedb

import (
	"fmt"
	assert2 "github.com/stretchr/testify/assert"
	"hash/crc32"
	"io/ioutil"
	"os"
	"testing"
	"time"
)

func TestDatafile(t *testing.T) {
	assert := assert2.New(t)
	directory, err := ioutil.TempDir("", "mldb")
	assert.NoError(err)
	defer os.RemoveAll(directory)

	var id int
	df, err := NewDatafile(directory, id, false)
	if assert.NoError(err) {
		//test that new os file is created with valid format
		t.Run("filePath", func(t *testing.T) {
			assert.Equal(df.Name(), fmt.Sprintf(datafileDefaultName, id))
		})

		//test writing
		t.Run("Write", func(t *testing.T) {
			tstamp := uint32(time.Date(2022, 12, 1, 0, 0, 0, 0, time.UTC).Unix())

			entry := generateEntry([]byte("testKey"), []byte("randomValue"), tstamp)
			offset, bytesWritten, err := df.Write(entry)
			assert.NoError(err)
			var expectedOffset int64
			assert.Equal(expectedOffset, offset)
			assert.Equal(entry.BytesToWrite(), bytesWritten)

			expectedOffset += bytesWritten

			entry = generateEntry([]byte("newtestKey"), []byte("newrandomValue"), tstamp)
			offset, bytesWritten, err = df.Write(entry)
			assert.NoError(err)
			assert.Equal(expectedOffset, offset)
			assert.Equal(entry.BytesToWrite(), bytesWritten)
		})

		t.Run("Read", func(t *testing.T) {
			tstamp := uint32(time.Date(2022, 12, 1, 0, 0, 0, 0, time.UTC).Unix())
			entry := generateEntry([]byte("testKey"), []byte("randomValue"), tstamp)
			offset, bytesWritten, err := df.Write(entry)
			assert.NoError(err)

			readEntry, bytesRead, readErr := df.ReadFrom(uint32(offset), uint32(bytesWritten))
			assert.NoError(readErr)
			assert.Equal(entry, readEntry)
			assert.Equal(bytesWritten, bytesRead)
		})
	}
}

func generateEntry(key, value []byte, tstamp uint32) Entry {
	return Entry{
		Checksum:  crc32.ChecksumIEEE(value),
		Tstamp:    tstamp,
		KeySize:   uint16(len(key)),
		ValueSize: uint32(len(value)),
		Key:       key,
		Value:     value,
	}
}
