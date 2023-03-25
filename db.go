package memorylanedb

import (
	"errors"
	"io/fs"
	"os"
	"sync"
	"syscall"
)

type Option struct {
}

type DB struct {
	instanceFD uintptr
	mu         sync.RWMutex      // use rw mutex for multiple readers to read the state
	state      map[Key]ValueMeta // map is not concurrent safe
}

func NewDB(path string, opts Option) (*DB, error) {
	fd, err := open(path)
	if err != nil {
		return nil, err
	}
	db := DB{
		instanceFD: *fd,
	}
	return &db, nil
}

/**
Bitcask APIs
*/
func (db *DB) Put(key Key, value []byte) error {
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