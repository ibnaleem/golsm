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

	deduplicatedSlice := []memtable.Entry{}

	for i, entry := range allEntries {

		if (i == 0) {
			deduplicatedSlice = append(deduplicatedSlice, entry)
			continue
		}

		prevKey := allEntries[i - 1]
		currKey := allEntries[i]

		if (bytes.Equal(prevKey.Key, currKey.Key)) {
			continue
		} else {
			deduplicatedSlice = append(deduplicatedSlice, entry)
		}

	}

}