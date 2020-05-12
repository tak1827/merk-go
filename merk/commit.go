package merk

// TODO: Make this option
const DafaultLevels = 1

type Commiter struct {
	batch  WriteBatch
	height uint8
	levels uint8
}

func newCommitter(batch WriteBatch, height, levels uint8) *Commiter {
	return &Commiter{batch, height, levels}
}

func (c *Commiter) write(tree *Tree) error {
	// Node: allow for testing
	if c.batch == nil {
		return nil
	}

	var key []byte = tree.key()

	b, err := tree.marshal()
	if err != nil {
		return err
	}

	if err := c.batch.put(append(NodeKeyPrefix, key...), b); err != nil {
		return err
	}

	return nil
}

func (c *Commiter) prune(tree *Tree) bool {
	return c.height-tree.height() >= c.levels
}
