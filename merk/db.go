package merk

import (
	"io"
)

const (
	DefaultDBDir = "../storage/merkdb"
)

var (
	RootKey       = []byte(".root")
	NodeKeyPrefix = []byte("@1:")
	gDB           DB
)

type DB interface {
	io.Closer

	Destroy() error

	Dir() string

	get(key []byte) ([]byte, error)
	put(key, value []byte) error
	delete(key []byte) error

	newWriteBatch() WriteBatch
	commitWriteBatch(batch WriteBatch) error

	fetchTree(key []byte) (*Tree, error)
	fetchTrees(key []byte) (*Tree, error)
}

func dbExist() bool {
	return gDB != nil
}

func fetchTree(key []byte) (*Tree, error) {
	return gDB.fetchTree(key)
}

type WriteBatch interface {
	put(key, value []byte) error
	delete(key []byte) error

	cancel()
}

func newWriteBatch() WriteBatch {
	return gDB.newWriteBatch()
}

func commitWriteBatch(w WriteBatch) error {
	return gDB.commitWriteBatch(w)
}

type nullLog struct{}

func (l nullLog) Errorf(f string, v ...interface{})   {}
func (l nullLog) Warningf(f string, v ...interface{}) {}
func (l nullLog) Infof(f string, v ...interface{})    {}
func (l nullLog) Debugf(f string, v ...interface{})   {}
