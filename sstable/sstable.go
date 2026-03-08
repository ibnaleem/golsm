package sstable

import (
	"time"

	"github.com/bits-and-blooms/bloom"
)

type SSTable struct {
	path        string
	timestamp   time.Time
	bloomFilter *bloom.BloomFilter
}

func New(path string) *SSTable {
	now := time.Now()
	
	n := uint(1000000)
	fp := 0.01 // 1% false positive rate

	bf := bloom.NewWithEstimates(n, fp)

	return &SSTable{
		path: path,
		timestamp: now,
		bloomFilter: bf,
	}

}