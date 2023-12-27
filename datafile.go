package memorylanedb

import (
	"fmt"
	"os"
	"path/filepath"
)

var datafileDefaultName = "%04d" + DATAFILE_SUFFIX
var mergedDatafileDefaultName = "%04d" + MERGED_DATAFILE_SUFFIX

type EntryWithOffset struct {
	Entry
	Offset uint32
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
	CreateIterator() Iterator[EntryWithOffset]
}

type datafile struct {
	id         int
	name       string
	file       *os.File
	offset     int64 // byte offset to track writes
	codec      *Codec
	readOnly   bool
	mergedFile bool
}

type DataFileOptions func(df *datafile)

func AsReadOnly() DataFileOptions {
	return func(df *datafile) {
		df.readOnly = true
	}
}

func AsMergedFile() DataFileOptions {
	return func(df *datafile) {
		df.mergedFile = true
	}
}

// implement iterator pattern
type datafileIterator struct {
	current_offset int64
	df             *datafile
}

func (dfi *datafileIterator) hasNext() bool {
	return dfi.current_offset < dfi.df.Size()
}

func (dfi *datafileIterator) getNext() (EntryWithOffset, error) {
	entry, bytesRead, err := dfi.df.Read()
	if err != nil {
		return EntryWithOffset{}, err
	}
	entryWithOffset := EntryWithOffset{
		entry,
		uint32(dfi.current_offset),
	}
	dfi.current_offset += bytesRead
	return entryWithOffset, nil
}

func NewDatafile(directory string, id int, opts ...DataFileOptions) (Datafile, error) {
	// create new datafile
	df := &datafile{}
	for _, o := range opts {
		o(df)
	}

	name := fmt.Sprintf(datafileDefaultName, id)
	if df.mergedFile {
		name = fmt.Sprintf(mergedDatafileDefaultName, id)
	}
	path := filepath.Join(directory, name)
	var f *os.File
	var fErr error

	if df.readOnly {
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

	df.id = id
	df.name = stat.Name()
	df.file = f
	df.offset = stat.Size()
	df.codec = codec

	return df, nil
}

func (df *datafile) CreateIterator() Iterator[EntryWithOffset] {
	return &datafileIterator{
		current_offset: 0,
		df:             df,
	}
}

func (df *datafile) ID() int {
	return df.id
}

func (df *datafile) Name() string {
	return df.name
}

func (df *datafile) Type() string {
	if df.mergedFile {
		return "merged-datafile"
	} else {
		return "datafile"
	}
}

func (df *datafile) Size() int64 {
	return df.offset
}

func (df *datafile) Write(entry Entry) (offset_before_write int64, bytesWritten int64, err error) {
	if df.readOnly {
		err = ErrReadOnlyDataFile
		return
	}
	offset_before_write = df.offset
	bytesWritten, err = df.codec.EncodeEntry(&entry)
	df.offset += bytesWritten
	return
}

func (df *datafile) Read() (entry Entry, bytesRead int64, err error) {
	// codec has the filehandler, so can keep track of reads
	// decode single entry from underlying data file
	bytesRead, err = df.codec.DecodeEntry(&entry)
	return
}

func (df *datafile) ReadFrom(index, size uint32) (entry Entry, bytesRead int64, err error) {
	buf := make([]byte, size)
	_, err = df.file.ReadAt(buf, int64(index))
	if err != nil {
		return
	}
	bytesRead, err = df.codec.DecodeSingleEntry(buf, &entry)
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
