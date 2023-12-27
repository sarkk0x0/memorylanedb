package memorylanedb

import (
	"hash/crc32"
	"time"
)

const (
	// In bytes
	CRC_SIZE     = 4
	TSSTAMP_SIZE = 4
	KEY_SIZE     = 2
	VALUE_SIZE   = 4
)

type Entry struct {
	Checksum  uint32
	Tstamp    uint32
	KeySize   uint16
	ValueSize uint32 // size of value in bytes
	Key       []byte
	Value     []byte
}

func NewEntry(key, value []byte) Entry {
	return Entry{
		Checksum:  crc32.ChecksumIEEE(value),
		Tstamp:    uint32(time.Now().Unix()),
		KeySize:   uint16(len(key)),
		ValueSize: uint32(len(value)),
		Key:       key,
		Value:     value,
	}
}

func (e *Entry) HeaderSize() int64 {
	return CRC_SIZE + TSSTAMP_SIZE + KEY_SIZE + VALUE_SIZE
}

func (e *Entry) Size() int64 {
	return e.HeaderSize() + int64(len(e.Key)+len(e.Value))
}

func (e *Entry) produceRecord(id int, offset, size uint32) (Key, EntryItem) {
	key := Key(e.Key)
	entryItem := EntryItem{
		fileId:      uint(id),
		tstamp:      e.Tstamp,
		entrySize:   size,
		entryOffset: offset,
	}
	return key, entryItem
}

func (e *Entry) toHint() *Hint {
	return &Hint{
		Tstamp:    e.Tstamp,
		KeySize:   e.KeySize,
		ValueSize: e.ValueSize,
		Key:       e.Key,
	}
}
