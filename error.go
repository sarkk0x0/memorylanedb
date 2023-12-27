package memorylanedb

import "errors"

var (
	ErrWriterNotExposed = errors.New("can not write for the datafile")

	ErrDBPathNotDir = errors.New("database path is not a directory")
	ErrDBPathInUse  = errors.New("database path is in use by another process")

	ErrKeyZeroLength       = errors.New("zero key length")
	ErrKeyGreaterThanMax   = errors.New("key size is greater than configured threshold")
	ErrValueGreaterThanMax = errors.New("value size is greater than configured threshold")

	ErrKeyNotFound = errors.New("key not found")

	ErrCorruptedData    = errors.New("value failed checksum check")
	ErrReadOnlyDataFile = errors.New("datafile is readonly")

	ErrWritingPrefix = errors.New("error writing entry prefix")
	ErrWritingKey    = errors.New("error writing key")
	ErrWritingValue  = errors.New("error writing value")

	ErrorNilEncoding = errors.New("error encoding nil entry")
	ErrorNilDecoding = errors.New("error decoding nil entry")
)
