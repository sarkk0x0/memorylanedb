package memorylanedb

import "errors"

var (
	ErrDBPathNotDir = errors.New("database path is not a directory")
	ErrDBPathInUse  = errors.New("database path is in use by another process")
)
