package memorylanedb

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"syscall"
)

type Option struct {
}

const (
	DATAFILE_SUFFIX = ".datafile"
	HINTFILE_SUFFIX = ".hintfile"
	TOMBSTONE_VALUE = "XXXX"
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
		path:       path,
		instanceFD: *fd,
		state:      state,
	}
	loadErr := db.loadDB()
	if loadErr != nil {
		loadErr = db.Close()
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
	if err := db.put([]byte(key), value); err != nil {
		return err
	}
	return nil
}

func (db *DB) Get(key Key) ([]byte, error) {
	item, ok := db.state[key]
	if !ok {
		return nil, ErrKeyNotFound
	}
	fileID := int(item.fileId)
	var df Datafile

	if fileID == db.activeDataFile.ID() {
		df = db.activeDataFile
	} else {
		df, ok = db.immutableDataFiles[fileID]
		if !ok {
			return nil, ErrKeyNotFound
		}
	}
	entry, _, err := df.ReadFrom(item.valueOffset, item.valueSize)
	if err != nil {
		return nil, err
	}
	return entry.Value, nil

}

func (db *DB) Has(key Key) (bool, error) {
	_, ok := db.state[key]
	return ok, nil
}

func (db *DB) Delete(key Key) error {
	// write tombstone value in datafile
	if err := db.Put(key, []byte(TOMBSTONE_VALUE)); err != nil {
		return err
	}
	// delete key from state
	delete(db.state, key)
	return nil
}

func (db *DB) sync() error {
	return db.activeDataFile.Sync()
}

func (db *DB) Close() error {
	for _, df := range db.immutableDataFiles {
		if err := df.Close(); err != nil {
			return err
		}
	}

	err := db.activeDataFile.Close()
	if err != nil {
		return err
	}
	return syscall.Flock(int(db.instanceFD), syscall.LOCK_UN)
}

func (db *DB) loadDB() error {
	/*
		Load the DB from the datafiles and hintfiles
	*/
	// find all datafiles in path by globbing
	filenames, err := filepath.Glob(fmt.Sprintf("%s/*.%s", db.path, DATAFILE_SUFFIX))
	sort.Strings(filenames)
	if err != nil {
		return err
	}
	hintfiles, err := filepath.Glob(fmt.Sprintf("%s/*.%s", db.path, HINTFILE_SUFFIX))
	hintfileIDs := ExtractIDsFromFilenames(hintfiles)

	activeDataFileID := 0
	for _, fn := range filenames {
		// for each datafile, check if it has a hintfile
		id, err := extractIDFromFilename(fn)
		if err != nil {
			continue
		}
		df, err := NewDatafile(db.path, id, true)
		db.immutableDataFiles[id] = df

		// if hintfile, load keydir from hintfile
		if Contains(id, hintfileIDs) {
			hf, err := OpenHintfile(db.path, id)
			if err != nil {
				return err
			}
			for {
				hint, hintErr := hf.Read()
				if hintErr != nil {
					if hintErr == io.EOF {
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
			if err != nil {
				return err
			}
			var offset uint32
			for {
				entry, bytesRead, entryErr := df.Read()
				if entryErr != nil {
					if entryErr == io.EOF {
						break
					} else {
						return entryErr
					}
				}
				// map entry to keydir entry
				key, entryItem := entry.produceRecord(id, offset, uint32(bytesRead))
				db.state[key] = entryItem

				offset += uint32(bytesRead)
			}
		}
		activeDataFileID = id
	}
	aDf, aErr := NewDatafile(db.path, activeDataFileID, false)
	if aErr != nil {
		return aErr
	}
	db.activeDataFile = aDf

	return nil
}

func (db *DB) put(key, value []byte) error {
	// check if active file needs rotation
	if err := db.rotateActiveFile(); err != nil {
		return err
	}
	entry := NewEntry(key, value)
	offset, bytesWritten, err := db.activeDataFile.Write(entry)
	if err != nil {
		return err
	}
	K, entryItem := entry.produceRecord(db.activeDataFile.ID(), uint32(offset), uint32(bytesWritten))
	db.state[K] = entryItem
	return nil
}

func (db *DB) rotateActiveFile() error {
	if db.activeDataFile.Size() < MAX_DATAFILE_SIZE {
		return nil
	}
	// close activeFile
	if err := db.activeDataFile.Close(); err != nil {
		return err
	}
	// add activefile to immutable datafiles
	currID := db.activeDataFile.ID()
	df, err := NewDatafile(db.path, currID, true)
	if err != nil {
		return err
	}
	db.immutableDataFiles[currID] = df
	// create new activefile
	newDf, err := NewDatafile(db.path, currID+1, false)
	if err != nil {
		return err
	}

	// set current activefile
	db.activeDataFile = newDf
	return nil
}

func (db *DB) merge() {

}

func open(path string) (*uintptr, error) {
	fileInfo, err := os.Stat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			err = os.Mkdir(path, fs.ModeDir|fs.ModePerm)
			if err != nil {
				return nil, err
			}
			fileInfo, _ = os.Stat(path)
		} else {
			return nil, err
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
