package memorylanedb

import (
	assert2 "github.com/stretchr/testify/assert"
	"io/ioutil"
	"os"
	"testing"
)

func TestAll(t *testing.T) {
	// Todo
	// Test can open directory, create directory if not exist
	// Test can not open directory is opened by another mldb process
	// Test can create new DB instance
	// Test close DB instance releases the path file lock

	var db *DB
	var err error

	assert := assert2.New(t)
	directory, err := ioutil.TempDir("", "mldb")
	assert.NoError(err)
	defer os.RemoveAll(directory)

	t.Run("Open", func(t *testing.T) {
		db, err = NewDB(directory, Option{})
		assert.NoError(err)
	})

	t.Run("Put", func(t *testing.T) {
		err = db.Put("foo", []byte("bar"))
		assert.NoError(err)
	})

	t.Run("Get", func(t *testing.T) {
		value, err := db.Get("foo")
		assert.NoError(err)
		assert.Equal([]byte("bar"), value)
	})
}
