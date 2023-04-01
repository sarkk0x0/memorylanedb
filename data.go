package memorylanedb

import (
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"time"
)

var ErrWriterNotExposed = errors.New("can not write for the datafile")

var datafileDefaultName = "%04d" + DATAFILE_SUFFIX

type Entry struct {
	Checksum  uint32
	Tstamp    uint32
	KeySize   uint16
	ValueSize uint32
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

func (e *Entry) produceRecord(id int, offset, size uint32) (Key, EntryItem) {
	key := Key(e.Key)
	entryItem := EntryItem{
		fileId:      uint(id),
		tstamp:      e.Tstamp,
		valueSize:   size,
		valueOffset: offset,
	}
	return key, entryItem
}

func (e *Entry) BytesToWrite() int64 {
	return int64(4 + 4 + 2 + 4 + uint32(e.KeySize) + e.ValueSize)
}

type Datafile interface {
	ID() int
	Name() string
	Write(Entry) (int64, int64, error)
	Read() (Entry, int64, error)
	Close() error
	Size() int64
	Sync() error
	ReadFrom(index, size uint32) (Entry, int64, error)
}

type datafile struct {
	id     int
	name   string
	file   *os.File
	offset int64 // byte offset to track writes
	codec  *Codec
}

func NewDatafile(directory string, id int, readonly bool) (Datafile, error) {
	// create new datafile
	name := fmt.Sprintf(datafileDefaultName, id)
	path := filepath.Join(directory, name)
	var f *os.File
	var fErr error
	if readonly {
		f, fErr = os.OpenFile(path, os.O_RDONLY, 0400)

	} else {
		f, fErr = os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
	}
	if fErr != nil {
		return nil, fErr
	}

	stat, err := f.Stat()
	if err != nil {
		return nil, err
	}

	codec := NewCodec(f)
	return &datafile{
		id:     id,
		name:   stat.Name(),
		file:   f,
		offset: stat.Size(),
		codec:  codec,
	}, nil
}

func (df *datafile) ID() int {
	return df.id
}

func (df *datafile) Name() string {
	return df.name
}

func (df *datafile) Type() string {
	return "datafile"
}

func (df *datafile) Size() int64 {
	return df.offset
}

func (df *datafile) Write(entry Entry) (offset int64, bytesWritten int64, err error) {
	// encode the entry in a binary format
	// |
	//
	offset = df.offset
	bytesWritten, err = df.codec.EncodeEntry(&entry)
	df.offset += bytesWritten
	return
}

func (df *datafile) Read() (entry Entry, bytesRead int64, err error) {
	// codec has the filehandler, so can keep track of reads
	bytesRead, err = df.codec.DecodeEntry(&entry)
	return
}

func (df *datafile) ReadFrom(index, size uint32) (entry Entry, bytesRead int64, err error) {
	buf := make([]byte, size)
	br, err := df.file.ReadAt(buf, int64(index))
	bytesRead = int64(br)
	err = df.codec.DecodeSingleEntry(buf, &entry)
	return
}

func (df *datafile) Close() error {
	// flush from in-memory fs cache to disk
	err := df.Sync()
	if err != nil {
		return err
	}
	return df.file.Close()

}

func (df *datafile) Sync() error {
	// flush from in-memory fs cache to disk
	return df.file.Sync()
}
