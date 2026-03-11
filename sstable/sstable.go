package sstable

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/bits-and-blooms/bloom"
	"github.com/ibnaleem/golsm/memtable"
)

func check(e error) {
    if e != nil {
        panic(e)
    }
}

type IndexEntry struct {
    Key    []byte
    Offset uint64
}

type SSTable struct {
	filepath      string
	indexFilePath string
	timestamp     time.Time
	bloomFilter   *bloom.BloomFilter
}

func New(path string) *SSTable {
	now := time.Now()
	
	ext       := filepath.Ext(path)
	base      := strings.TrimSuffix(path, ext)
	indexFile := base + ".index"
	

	n := uint(1000000)
	fp := 0.01 // 1% false positive rate

	bf := bloom.NewWithEstimates(n, fp)

	return &SSTable{
		filepath: path,
		indexFilePath: indexFile,
		timestamp: now,
		bloomFilter: bf,
	}

}

func (s *SSTable) Write(entries []memtable.Entry) {

	f, err := os.OpenFile(s.filepath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	check(err)

	defer f.Close()

	writer := bufio.NewWriter(f)

	currOffset := uint64(0)
	indexEntries := []IndexEntry{}

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


		indexEntry := IndexEntry{
			Key: entry.Key,
			Offset: currOffset,
		}

		indexEntries = append(indexEntries, indexEntry)

		currOffset += uint64(8) + uint64(len(entry.Key)) + uint64(len(entry.Value))
	}

	writer.Flush()

	indexFile, err := os.OpenFile(s.indexFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	check(err)

	defer indexFile.Close()

	indexWriter := bufio.NewWriter(indexFile)

	for _, indexEntry := range indexEntries {
		err := binary.Write(indexWriter, binary.LittleEndian, uint32(len(indexEntry.Key))) 
		check(err)

		err = binary.Write(indexWriter, binary.LittleEndian, indexEntry.Key)
		check(err)

		err = binary.Write(indexWriter, binary.LittleEndian, indexEntry.Offset)
		check(err)
	}

	indexWriter.Flush()
}

func (s *SSTable) Read(key []byte) []byte {
	
	if (!s.bloomFilter.Test(key)) {
		return nil
	}
		
	f, err := os.OpenFile(s.indexFilePath, os.O_RDONLY, 0644)
	check(err)

	defer f.Close()

	buff := bufio.NewReader(f)

	indexEntries := []IndexEntry{}
	var keyLen uint32
	var offset uint64

	for {
		err := binary.Read(buff, binary.LittleEndian, &keyLen)

		if err == io.EOF {
			break
		} else if err != nil {
			check(err)
		}

		keyBytes := make([]byte, keyLen)

		err = binary.Read(buff, binary.LittleEndian, keyBytes)

		if err == io.EOF {
			break
		} else if err != nil {
			check(err)
		}

		err = binary.Read(buff, binary.LittleEndian, &offset)

		if err == io.EOF {
			break
		} else if err != nil {
			check(err)
		}
	
		indexEntry := IndexEntry{
			Key: keyBytes,
			Offset: offset,
		}
		
		indexEntries = append(indexEntries, indexEntry)
	}

	l := 0
		r := len(indexEntries)

		for l < r {

			mid := (l + r) / 2

			comparison := bytes.Compare(key, indexEntries[mid].Key)

			if (comparison == 0) {

				offset := indexEntries[mid].Offset

				f, err := os.OpenFile(s.filepath, os.O_RDONLY, 0644)
				check(err)

				defer f.Close()

				f.Seek(int64(offset), io.SeekStart)

				buff := bufio.NewReader(f)

				err = binary.Read(buff, binary.LittleEndian, &keyLen)
				check(err)

				keyBytes := make([]byte, keyLen)

				err = binary.Read(buff, binary.LittleEndian, keyBytes)
				check(err)

				var valueLen uint32

				err = binary.Read(buff, binary.LittleEndian, &valueLen)
				check(err)

				valueBytes := make([]byte, valueLen)

				err = binary.Read(buff, binary.LittleEndian, valueBytes)
				check(err)

				return valueBytes
			} else if (comparison == -1) {
				r = mid - 1
			} else {
				l = mid + 1
			}

		}

		return nil
}

func (s *SSTable) ReadAll() []memtable.Entry {
	
	f, err := os.OpenFile(s.filepath, os.O_RDONLY, 0644)
	check(err)

	defer f.Close()

	buff := bufio.NewReader(f)

	entries := []memtable.Entry{}

	for {
		var keyLen uint32

		err := binary.Read(buff, binary.LittleEndian, &keyLen)

		if err == io.EOF {
			break
		} else if err != nil {
			check(err)
		}

		keyBytes := make([]byte, keyLen)
		err = binary.Read(buff, binary.LittleEndian, keyBytes)
		if err == io.EOF {
			break
		} else if err != nil {
			check(err)
		}

		var valueLen uint32
		err = binary.Read(buff, binary.LittleEndian, &valueLen)

		if err == io.EOF {
			break
		} else if err != nil {
			check(err)
		}

		valueBytes := make([]byte, valueLen)
		err = binary.Read(buff, binary.LittleEndian, valueBytes)

		if err == io.EOF {
			break
		} else if err != nil {
			check(err)
		}

		entry := memtable.Entry{
			Key: keyBytes,
			Value: valueBytes,
		}

		entries = append(entries, entry)

	}

	return entries
}