package memorylanedb

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
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

func (e *Entry) produceRecord(id int) (Key, EntryItem) {
	key := Key(e.Key)
	entryItem := EntryItem{
		fileId:    uint(id),
		valueSize: e.ValueSize,
		tstamp:    e.Tstamp,
	}
	return key, entryItem
}

type Datafile interface {
	ID() int
	Name() string
	Write(Entry) (int64, int64, error)
	Read() (Entry, int64, error)
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
