package memorylanedb

const (
	// In bytes
	VALUE_OFFSET_SIZE = 4
)

type Hint struct {
	Tstamp      uint32
	KeySize     uint16
	ValueSize   uint32
	ValueOffset uint32 // offset of the entry in the datafile
	Key         []byte
}

func (h *Hint) HeaderSize() int64 {
	return TSSTAMP_SIZE + KEY_SIZE + VALUE_SIZE + VALUE_OFFSET_SIZE
}

func (h *Hint) Size() int64 {
	return h.HeaderSize() + int64(len(h.Key))
}

func (h *Hint) produceRecord(id int) (Key, EntryItem) {
	key := Key(h.Key)
	entryItem := EntryItem{
		fileId:      uint(id),
		entrySize:   h.ValueSize,
		entryOffset: h.ValueOffset,
		tstamp:      h.Tstamp,
	}
	return key, entryItem
}
