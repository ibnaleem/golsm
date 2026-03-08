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

	newSize := m.size + len(key) + len(value)

	entry := Entry{
		Key: key,
		Value: value,
	}

	result := m.tree.ReplaceOrInsert(entry)

	if result == nil {
		m.size = newSize
	} else {
		old := result.(Entry)
		m.size = newSize - len(old.Key)  - len(old.Value)
	}
}

func (m *Memtable) Get(key []byte) []byte {

	entry := Entry{Key: key, Value: nil}

	result := m.tree.Get(entry)

	if result == nil {
		return nil
	} else {
		return result.(Entry).Value
	}
}

func (m *Memtable) Delete(key []byte) {

	entry := Entry{Key: key, Value: nil}

	result := m.tree.Delete(entry)

	if result == nil {
		return
	} else {
		old := result.(Entry)
		m.size = m.size - len(old.Key) - len(old.Value)
	}
}