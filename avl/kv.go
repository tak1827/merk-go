package avl

import (
	badger "github.com/dgraph-io/badger/v2"
	"github.com/dgraph-io/badger/v2/options"
	"os"
	"github.com/pkg/errors"
	"io"
)

var (
	ErrNotFound = errors.New("not found")
)

type KV interface {
	io.Closer

  Get(key []byte) ([]byte, error)

  Put(key, value []byte) error

  NewWriteBatch() WriteBatch
  CommitWriteBatch(batch WriteBatch) error

  Delete(key []byte) error
}

type badgerKV struct {
	dir string
	db  *badger.DB
}

type nullLog struct{}

func (l nullLog) Errorf(f string, v ...interface{})   {}
func (l nullLog) Warningf(f string, v ...interface{}) {}
func (l nullLog) Infof(f string, v ...interface{})    {}
func (l nullLog) Debugf(f string, v ...interface{})   {}

func NewBadger(dir string) (*badgerKV, error) { // nolint:golint
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	// Explicitly specify compression. Because the default compression with CGO is ZSTD, and without CGO it's Snappy.
	db, err := badger.Open(badger.DefaultOptions(dir).WithLogger(nullLog{}).WithCompression(options.Snappy))
	if err != nil {
		return nil, err
	}

	b := &badgerKV{
		dir:     dir,
		db:      db,
	}

	return b, nil
}

func (b *badgerKV) Close() error {
	return b.db.Close()
}

func (b *badgerKV) Get(key []byte) ([]byte, error) {
	var value []byte

	err := b.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}

		value, err = item.ValueCopy(nil)
		return err
	})

	if err != nil {
		return nil, errors.Wrap(ErrNotFound, err.Error())
	}

	if value == nil {
		value = []byte{}
	}

	return value, err
}

func (b *badgerKV) Put(key, value []byte) error {
	return b.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, value)
	})
}

func (b *badgerKV) Delete(key []byte) error {
	return b.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(key)
	})
}

func (b *badgerKV) CommitWriteBatch(batch WriteBatch) error {
	wb, ok := batch.(*badgerWriteBatch)
	if !ok {
		return errors.New("badger: not fed in a proper badger write batch")
	}

	return wb.batch.Flush()
}

type badgerWriteBatch struct {
	batch *badger.WriteBatch
}

type WriteBatch interface {
	Put(key, value []byte) error
	Delete(key []byte) error

	Destroy()
}


func (b *badgerKV) NewWriteBatch() WriteBatch {
	return &badgerWriteBatch{
		batch: b.db.NewWriteBatch(),
	}
}

func (b *badgerWriteBatch) Put(key, value []byte) error {
	k := append([]byte{}, key...)
	v := append([]byte{}, value...)

	return b.batch.Set(k, v)
}

func (b *badgerWriteBatch) Delete(key []byte) error {
	return b.batch.Delete(key)
}

func (b *badgerWriteBatch) Destroy() {
	b.batch.Cancel()
}
