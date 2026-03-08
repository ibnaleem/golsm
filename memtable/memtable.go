package memtable

import (
	"bytes"

	"github.com/google/btree"
)

type Entry struct {
	Key []byte
	Value []byte
}

func (e Entry) Less(other btree.Item) bool {

	comparison := bytes.Compare(e.Key, other.(Entry).Key)

	return comparison < 0

}

type Memtable struct {
	tree *btree.BTree
	size int
}

func New() *Memtable {
    return &Memtable{
        tree: btree.New(32),
        size: 0,
    }
}

func (m *Memtable) Put(key []byte, value []byte) {

	m.size = len(key) + len(value) + m.size

	entry := Entry{
		Key: key,
		Value: value,
	}

	m.tree.ReplaceOrInsert(entry)

}