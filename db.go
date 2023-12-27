package memorylanedb

import (
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
)

type Option struct {
}

type foldFunc func(key Key) error

const (
	DATAFILE_SUFFIX        = ".datafile"
	MERGED_DATAFILE_SUFFIX = ".datafile.merged"
	HINTFILE_SUFFIX        = ".hintfile"
	TOMBSTONE_VALUE        = "XXXX"
)

type DB struct {
	path               string
	instanceFD         uintptr
	mu                 sync.RWMutex      // use rw mutex for multiple readers to read the state
	keyDir             map[Key]EntryItem // map is not concurrent safe
	activeDataFile     Datafile
	immutableDataFiles map[int]Datafile // maps file ids to datafiles
	maxFileId          int
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
		keyDir:     state,
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
	item, ok := db.keyDir[key]
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
	entry, _, err := df.ReadFrom(item.entryOffset, item.entrySize)
	if err != nil {
		return nil, err
	}
	value := entry.Value
	if entry.Checksum != crc32.ChecksumIEEE(value) {
		return nil, ErrCorruptedData
	}
	return value, nil

}

func (db *DB) Has(key Key) (bool, error) {
	_, ok := db.keyDir[key]
	return ok, nil
}

func (db *DB) Delete(key Key) error {
	// write tombstone value in datafile
	if err := db.Put(key, []byte(TOMBSTONE_VALUE)); err != nil {
		return err
	}
	// delete key from state
	delete(db.keyDir, key)
	return nil
}

func (db *DB) Fold(f foldFunc) error {
	db.mu.RLock()
	defer db.mu.RUnlock()
	for k := range db.keyDir {
		err := f(k)
		if err != nil {
			return err
		}
	}
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
		We don't load mergefiles, the assumption here is a mergefile must have a corresponding hintfile
	*/
	// find all datafiles in path by globbing
	filenames, err := filepath.Glob(fmt.Sprintf("%s/*%s*", db.path, DATAFILE_SUFFIX))
	sort.Strings(filenames)
	if err != nil {
		return err
	}
	hintfiles, err := filepath.Glob(fmt.Sprintf("%s/*%s", db.path, HINTFILE_SUFFIX))
	if err != nil {
		return err
	}
	hintfileIDs := ExtractIDsFromFilenames(hintfiles)

	activeDataFileID := 0
	for _, fn := range filenames {
		// for each datafile, check if it has a hintfile
		id, err := extractIDFromFilename(fn)
		if err != nil {
			continue
		}
		opts := []DataFileOptions{AsReadOnly()}
		if strings.Contains(fn, MERGED_DATAFILE_SUFFIX) {
			opts = append(opts, AsMergedFile())
		}
		df, err := NewDatafile(db.path, id, opts...)
		db.immutableDataFiles[id] = df

		// if hintfile, load keydir from hintfile
		if Contains(id, hintfileIDs) {
			hf, err := NewHintfile(db.path, id)
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
				db.keyDir[key] = entryItem
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
				db.keyDir[key] = entryItem

				offset += uint32(bytesRead)
			}
		}
		activeDataFileID = id
	}
	aDf, aErr := NewDatafile(db.path, activeDataFileID)
	if aErr != nil {
		return aErr
	}
	db.activeDataFile = aDf
	db.maxFileId = activeDataFileID
	return nil
}

func (db *DB) put(key, value []byte) error {
	// check if active file needs rotation
	if err := db.rotateActiveFile(); err != nil {
		return err
	}
	entry := NewEntry(key, value)
	offset_before_write, bytesWritten, err := db.activeDataFile.Write(entry)
	if err != nil {
		return err
	}
	K, entryItem := entry.produceRecord(db.activeDataFile.ID(), uint32(offset_before_write), uint32(bytesWritten))
	db.keyDir[K] = entryItem
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
	df, err := NewDatafile(db.path, currID, AsReadOnly())
	if err != nil {
		return err
	}
	db.immutableDataFiles[currID] = df
	// create new activefile
	// use max file id + 1
	newID := db.maxFileId + 1
	newDf, err := NewDatafile(db.path, newID)
	if err != nil {
		return err
	}

	// set current activefile
	db.activeDataFile = newDf
	db.maxFileId = newID

	return nil
}

func (db *DB) merge1() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	var mergefile Datafile
	var hintfile Hintfile
	var err error

	// get all immutable datafiles
	for fileId, df := range db.immutableDataFiles {

		datafileIterator := df.CreateIterator()
		for datafileIterator.hasNext() {
			entry, _ := datafileIterator.getNext()
			key := entry.Key
			entryItem, ok := db.keyDir[Key(key)]
			if !ok {
				continue
			}
			if !(entryItem.fileId == uint(fileId) && entryItem.entryOffset == entry.Offset) {
				continue
			}
			if mergefile == nil {
				mergeFileId := db.maxFileId + 1
				mergefile, err = NewDatafile(db.path, mergeFileId, AsMergedFile())
				if err != nil {
					return err
				}
				db.maxFileId = mergeFileId
				hintfile, err = NewHintfile(db.path, mergefile.ID())
				if err != nil {
					return err
				}
			}
			// write entry in mergefile
			offset_before_write, _, err := mergefile.Write(entry.Entry)
			if err != nil {
				return err
			}
			hint := entry.Entry.toHint()
			hint.ValueOffset = uint32(offset_before_write)
			K, newEntryItem := hint.produceRecord(mergefile.ID())
			db.keyDir[K] = newEntryItem

			// write hint in hintfile
			_, err = hintfile.Write(*hint)
			if err != nil {
				return err
			}
		}
		err = df.Close()
		if err != nil {
			return err
		}
		delete(db.immutableDataFiles, fileId)
		err = os.Remove(filepath.Join(db.path, df.Name()))
		if err != nil {
			return err
		}
	}

	return nil
}
func (db *DB) merge() error {
	/*
		OPTION 1: Going via datafiles
			To implement merge,
			- loop through all immutable datafiles
			- For each record, check that the fileID and offset in df is same as entryItem in keydir
			- If it is the same, write entry in new merge file and update keydir
			- After reaching EOF on immutable df, close df and delete df
			- if merge file exceeds limit, rollover merge file.
			- merge file rollover also creates a corresponding hint file

		OPTION 2: Going via keydir
			This approach basically writes out keydir to disk
			- Open a temporary DB; a merge DB
			- Use the fold method to iterate over all keys
			- Read value from db keydir and write to mdb
	*/
	return db.merge1()

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
