package memorylanedb

import "errors"

var (
	ErrDBPathNotDir = errors.New("database path is not a directory")
	ErrDBPathInUse  = errors.New("database path is in use by another process")

	ErrKeyZeroLength       = errors.New("zero key length")
	ErrKeyGreaterThanMax   = errors.New("key size is greater than configured threshold")
	ErrValueGreaterThanMax = errors.New("value size is greater than configured threshold")
)
