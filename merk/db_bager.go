package merk

import (
	"errors"
	"fmt"
	badger "github.com/dgraph-io/badger/v2"
	"github.com/dgraph-io/badger/v2/options"
	"os"
)

var (
	_ DB         = (*badgerDB)(nil)
	_ WriteBatch = (*badgerWriteBatch)(nil)
)

type badgerDB struct {
	dir string
	db  *badger.DB
}

func newBadger(dir string) (DB, error) {
	if gDB != nil {
		return nil, fmt.Errorf("db already open, dir: %v", gDB.Dir())
	}

	if dir == "" {
		dir = DefaultDBDir
	}

	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	ops := setBadgerOpts(dir)

	db, err := badger.Open(ops)
	if err != nil {
		return nil, fmt.Errorf("failed to open db: %w", err)
	}

	gDB = &badgerDB{ops.Dir, db}

	return gDB, nil
}

func setBadgerOpts(dir string) badger.Options {
	// See available options
	// https://godoc.org/github.com/dgraph-io/badger#Options
	ops := badger.DefaultOptions(dir)

	// no logging
	ops = ops.WithLogger(nullLog{})

	// Explicitly specify compression
	// Because the default compression with CGO is ZSTD, and without CGO it's Snappy.
	ops = ops.WithCompression(options.Snappy)

	return ops
}

func (b *badgerDB) Close() error {
	gDB = nil
	return b.db.Close()
}

func (b *badgerDB) Dir() string {
	return b.dir
}

func (b *badgerDB) get(key []byte) ([]byte, error) {
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
		return nil, fmt.Errorf("failed to get from badger, %w", err)
	}

	return value, err
}

func (b *badgerDB) put(key, value []byte) error {
	return b.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, value)
	})
}

func (b *badgerDB) delete(key []byte) error {
	return b.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(key)
	})
}

func (b *badgerDB) Destroy() error {
	return b.db.DropAll()
}

func (b *badgerDB) newWriteBatch() WriteBatch {
	return &badgerWriteBatch{
		batch: b.db.NewWriteBatch(),
	}
}

func (b *badgerDB) commitWriteBatch(batch WriteBatch) error {
	wb, ok := batch.(*badgerWriteBatch)
	if !ok {
		return errors.New("badger: not fed in a proper badger write batch")
	}

	return wb.batch.Flush()
}

func (b *badgerDB) fetchTree(key []byte) (*Tree, error) {
	if key == nil {
		return nil, errors.New("empty key while fetching tree")
	}

	value, err := b.get(append(NodeKeyPrefix, key...))
	if err != nil {
		return nil, fmt.Errorf("failed get, %w", err)
	}

	t := unmarshalTree(value)

	return t, nil
}

func (b *badgerDB) fetchTrees(key []byte) (*Tree, error) {
	var h Hash

	tree, err := b.fetchTree(key)
	if err != nil {
		return nil, err
	}

	handler := func(isLeft bool, l Link) error {
		if l != nil {
			h = l.hash()
			t, err := b.fetchTrees(h[:])
			if err != nil {
				return err
			}

			tree.setLink(isLeft, l.intoStored(t))
		}
		return nil
	}

	if err := handler(true, tree.link(true)); err != nil {
		return nil, err
	}
	if err := handler(false, tree.link(false)); err != nil {
		return nil, err
	}

	return tree, nil
}

func (b *badgerDB) takeSnapshot() (Hash, error) {
	topKey, err := b.get(RootKey)
	if err != nil {
		return NullHash, fmt.Errorf("failed to get root key: %w", err)
	}

	tree, err := b.fetchTrees(topKey)
	if err != nil {
		return NullHash, fmt.Errorf("failed fetchTrees: %w", err)
	}

	wb := b.newWriteBatch()
	defer wb.cancel()

	committer := newCommitter(wb, 0, 0)
	if err := tree.commitsSnapshot(committer); err != nil {
		return NullHash, err
	}

	if err := b.commitWriteBatch(wb); err != nil {
		return NullHash, err
	}

	var h Hash
	copy(h[:], topKey)

	return h, nil
}

type badgerWriteBatch struct {
	batch *badger.WriteBatch
}

func (b *badgerWriteBatch) put(key, value []byte) error {
	return b.batch.Set(key, value)
}

func (b *badgerWriteBatch) delete(key []byte) error {
	return b.batch.Delete(key)
}

func (b *badgerWriteBatch) cancel() {
	b.batch.Cancel()
}
