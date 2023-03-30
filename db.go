package memorylanedb

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"syscall"
)

type Option struct {
}

const (
	DATAFILE_SUFFIX = ".datafile"
	HINTFILE_SUFFIX = ".hintfile"
)

type DB struct {
	path               string
	instanceFD         uintptr
	mu                 sync.RWMutex      // use rw mutex for multiple readers to read the state
	state              map[Key]EntryItem // map is not concurrent safe
	activeDataFile     Datafile
	immutableDataFiles map[int]Datafile // maps file ids to datafiles
}

func NewDB(path string, opts Option) (*DB, error) {
	fd, err := open(path)
	if err != nil {
		return nil, err
	}

	state := make(map[Key]EntryItem)
	db := DB{
		instanceFD: *fd,
		state:      state,
	}
	//Todo: load in-memory state from datafiles and hintfiles if existing
	loadErr := db.loadDB()
	if loadErr != nil {
		db.Close()
		return nil, loadErr
	}
	return &db, nil
}

/**
Bitcask APIs
*/
func (db *DB) Put(key Key, value []byte) error {
	// validate key length
	if key.length() == 0 {
		return ErrKeyZeroLength
	}
	// validate key and value sizes
	if key.length() > MAX_KEY_SIZE {
		return ErrKeyGreaterThanMax
	}
	if len(value) > MAX_VALUE_SIZE {
		return ErrValueGreaterThanMax
	}
	// append to active file

	// update keydir
	return nil
}

func (db *DB) Get(key Key) ([]byte, error) {
	return nil, nil
}

func (db *DB) Has(key Key) (bool, error) {
	return false, nil
}

func (db *DB) Delete(key Key) error {
	return nil
}

func (db *DB) merge() {

}

func (db *DB) sync() {

}

func (db *DB) append(key, value []byte) {
	// get active data file
}

func (db *DB) Close() error {
	err := syscall.Flock(int(db.instanceFD), syscall.LOCK_UN)

	return err
}

func open(path string) (*uintptr, error) {
	fileInfo, err := os.Stat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			err = os.Mkdir(path, fs.ModeDir|fs.ModePerm)
			if err != nil {

			}
		}
	}
	if !fileInfo.IsDir() {
		return nil, ErrDBPathNotDir
	}

	f, fErr := os.Open(path)
	if fErr != nil {
		return nil, fErr
	}
	fd := f.Fd()
	lockErr := syscall.Flock(int(fd), syscall.LOCK_EX|syscall.LOCK_NB)
	if lockErr != nil {
		if errors.Is(lockErr, syscall.EAGAIN) {
			lockErr = ErrDBPathInUse
		}
		f.Close()
		return nil, lockErr
	}

	return &fd, nil

}

func (db *DB) loadDB() error {
	/*
		Load the DB from the datafiles and hintfiles
	*/
	// find all datafiles in path by globbing
	filenames, err := filepath.Glob(fmt.Sprintf("%s/%s", db.path, DATAFILE_SUFFIX))
	if err != nil {
		return err
	}
	hintfiles, err := filepath.Glob(fmt.Sprintf("%s/%s", db.path, HINTFILE_SUFFIX))
	hintfileIDs := ExtractIDsFromFilenames(hintfiles)

	for _, fn := range filenames {
		// for each datafile, check if it has a hintfile
		id, err := extractIDFromFilename(fn)
		if err != nil {
			continue
		}
		// if hintfile, load keydir from hintfile
		if Contains(id, hintfileIDs) {
			hf, err := OpenHintfile(db.path, id)
			if err != nil {
				return err
			}
			for {
				hint, hintErr := hf.Read()
				if hintErr != nil {
					if errors.Is(hintErr, io.EOF) {
						break
					} else {
						return hintErr
					}
				}
				// map hint to keydir entry
				key, entryItem := hint.produceRecord(id)
				db.state[key] = entryItem
			}
			hf.Close()
		} else {
			// read entry from datafile directly
			df, err := NewDatafile(db.path, id, true)
			if err != nil {
				return err
			}
			var offset uint32
			for {
				entry, bytesRead, entryErr := df.Read()
				if entryErr != nil {
					if errors.Is(entryErr, io.EOF) {
						break
					} else {
						return entryErr
					}
				}
				// map entry to keydir entry
				key, entryItem := entry.produceRecord(id)
				entryItem.valueOffset = offset
				db.state[key] = entryItem

				offset += uint32(bytesRead)
			}
		}
	}
	return nil
}
