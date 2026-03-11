package compaction

import (
	"bytes"
	"slices"

	"github.com/ibnaleem/golsm/memtable"
	"github.com/ibnaleem/golsm/sstable"
)

func Compact(sstables []*sstable.SSTable, outputPath string) *sstable.SSTable {
	
	allEntries := []memtable.Entry{}
	
	for _, s := range sstables {
		allEntries = append(allEntries, s.ReadAll()...) 
	}

	slices.SortStableFunc(allEntries, func(a, b memtable.Entry) int {
		return bytes.Compare(a.Key, b.Key)
	})

}