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