package sstable

import (
	"os"
	"time"
	"bufio"
	"encoding/binary"

	"github.com/bits-and-blooms/bloom"
	"github.com/ibnaleem/golsm/memtable"
)

func check(e error) {
    if e != nil {
        panic(e)
    }
}

type SSTable struct {
	filepath        string
	timestamp   time.Time
	bloomFilter *bloom.BloomFilter
}

func New(path string) *SSTable {
	now := time.Now()

	n := uint(1000000)
	fp := 0.01 // 1% false positive rate

	bf := bloom.NewWithEstimates(n, fp)

	return &SSTable{
		filepath: path,
		timestamp: now,
		bloomFilter: bf,
	}

}

func (s *SSTable) Write(entries []memtable.Entry) {

	f, err := os.OpenFile(s.filepath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	check(err)

	defer f.Close()

	writer := bufio.NewWriter(f)

	for _, entry := range entries {
		err := binary.Write(writer, binary.LittleEndian, uint32(len(entry.Key)))
		check(err)

		err = binary.Write(writer, binary.LittleEndian, entry.Key)
		check(err)

		err = binary.Write(writer, binary.LittleEndian, uint32(len(entry.Value)))
		check(err)

		err = binary.Write(writer, binary.LittleEndian, entry.Value)
		check(err)

		s.bloomFilter.Add(entry.Key)
	}

	writer.Flush()
}