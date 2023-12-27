package memorylanedb

const (
	MAX_KEY_SIZE       = 512
	MAX_VALUE_SIZE     = 1024 * 1024 * 2   // 2MB
	MAX_DATAFILE_SIZE  = 1024 * 1024 * 10  // 10MB
	MAX_MERGEFILE_SIZE = 1024 * 1024 * 100 // 100MB
)

type Key string

func (k Key) length() int {
	return len(string(k))
}

type EntryItem struct {
	fileId      uint
	entrySize   uint32
	entryOffset uint32 // 32-bit, max offset of 2^32
	tstamp      uint32
}
