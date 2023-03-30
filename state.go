package memorylanedb

const (
	MAX_KEY_SIZE      = 512
	MAX_VALUE_SIZE    = 1024 * 1024 * 2  // 2MB
	MAX_DATAFILE_SIZE = 1024 * 1024 * 10 // 10MB
)

type Key string

func (k Key) length() int {
	return len(string(k))
}

type EntryItem struct {
	fileId      uint
	valueSize   uint32
	valueOffset uint32 // 32-bit, max offset of 2^32
	tstamp      uint32
}
