// +build gofuzz

package merk

const storageDir string = "../storage/fuzz"

func Fuzz(data []byte) int {
	m, db, err := New(storageDir)
	if err != nil {
		if db != nil {
			db.Close()
		}
		return 0
	}

	defer db.Close()

	var batch Batch = []*OP{
		&OP{O: Put, K: data, V: data},
	}

	if _, err := m.Apply(batch); err != nil {
		return 0
	}

	return 1
}
