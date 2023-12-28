package memorylanedb

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"

	assert2 "github.com/stretchr/testify/assert"
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
	directory := t.TempDir()

	t.Run("Open", func(t *testing.T) {
		db, err = NewDB(directory, nil)
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

func TestLoadDB(t *testing.T) {
	assert := assert2.New(t)
	directory := t.TempDir()

	t.Run("GlobDatafileAndMergeFiles", func(t *testing.T) {
		for i := 0; i < 3; i++ {
			path := fmt.Sprintf(directory+"/"+datafileDefaultName, i)
			_, err := os.Create(path)
			if err != nil {
				t.Fatal(err)
			}
			path = fmt.Sprintf(directory+"/"+mergedDatafileDefaultName, i)
			_, err = os.Create(path)
			if err != nil {
				t.Fatal(err)
			}
		}
		globPath := fmt.Sprintf("%s/*%s*", directory, DATAFILE_SUFFIX)
		filenames, err := filepath.Glob(globPath)
		assert.NoError(err)
		sort.Strings(filenames)
		assert.Len(filenames, 6)
	})

}
