package memorylanedb

const (
	MAX_KEY_SIZE      = 512
	MAX_DATAFILE_SIZE = 1024 * 1024 * 10 // 10MB
)

type Key string

type ValueMeta struct {
	fileId      uint
	valueSize   uint32
	valueOffset uint32 // 32-bit, max offset of 2^32
	tstamp      uint32
}
