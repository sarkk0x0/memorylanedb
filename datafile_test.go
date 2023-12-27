package memorylanedb

import (
	"testing"

	assert2 "github.com/stretchr/testify/assert"
)

func TestMultipleDatafiles(t *testing.T) {
	assert := assert2.New(t)

	tmpDir := t.TempDir()
	for i := 0; i < 3; i++ {
		df, err := NewDatafile(tmpDir, 1)
		t.Log(df.Name())
		assert.NoError(err)
	}

}
