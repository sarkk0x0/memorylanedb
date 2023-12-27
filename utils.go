package memorylanedb

import (
	"errors"
	"path/filepath"
	"strconv"
	"strings"
)

type Iterator[T any] interface {
	getNext() (T, error)
	hasNext() bool
}

func extractIDFromFilename(filename string) (int, error) {
	basefn := filepath.Base(filename)
	ext := filepath.Ext(basefn)
	if !Contains(ext, []string{"datafile", "hintfile"}) {
		return -1, errors.New("invalid file extension")
	}
	fileIDString := strings.TrimSuffix(basefn, ext)
	parts := strings.Split(fileIDString, ".")
	idPart := parts[0]
	return strconv.Atoi(idPart)
}

func ExtractIDsFromFilenames(filenames []string) []int {
	ids := make([]int, 0)
	for _, fn := range filenames {
		id, err := extractIDFromFilename(fn)
		if err != nil {
			continue
		}
		ids = append(ids, id)
	}
	return ids
}

func Contains[T comparable](s T, opts []T) bool {
	for _, v := range opts {
		if s == v {
			return true
		}
	}
	return false
}
