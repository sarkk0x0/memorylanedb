package memorylanedb

import (
	"fmt"
	"os"
	"path/filepath"
)

var hintfileDefaultName = "%04d" + HINTFILE_SUFFIX

type Hint struct {
	Tstamp      uint32
	KeySize     uint16
	ValueSize   uint32
	ValueOffset uint32
	Key         []byte
}

func (h *Hint) produceRecord(id int) (Key, EntryItem) {
	key := Key(h.Key)
	entryItem := EntryItem{
		fileId:      uint(id),
		valueSize:   h.ValueSize,
		valueOffset: h.ValueOffset,
		tstamp:      h.Tstamp,
	}
	return key, entryItem
}

type Hintfile interface {
	ID() int
	Name() string
	Write(hint Hint) (int64, error)
	Read() (Hint, error)
	Close() error
}

// generated after merge and compaction
type hintfile struct {
	id    int
	name  string
	file  *os.File
	codec *Codec
}

func OpenHintfile(directory string, id int) (Hintfile, error) {
	// opens a hintfile
	name := fmt.Sprintf(hintfileDefaultName, id)
	path := filepath.Join(directory, name)
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	stat, err := f.Stat()
	if err != nil {
		return nil, err
	}

	codec := NewCodec(f)
	return &hintfile{
		id:    id,
		name:  stat.Name(),
		file:  f,
		codec: codec,
	}, nil
}

func (h *hintfile) ID() int {
	return h.id
}

func (h *hintfile) Name() string {
	return h.name
}

func (h *hintfile) Write(hint Hint) (int64, error) {
	bytesWritten, err := h.codec.EncodeHint(&hint)
	return bytesWritten, err
}

func (h *hintfile) Read() (hint Hint, err error) {
	// codec has the filehandler, so can keep track of reads
	err = h.codec.DecodeHint(&hint)
	return
}

func (h *hintfile) Close() error {
	return h.file.Close()
}
