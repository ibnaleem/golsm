package wal

import (
	"bufio"
	"encoding/gob"
	"io"
	"os"
)

type Operation string

const (
    PutOperation Operation = "PUT"
    DeleteOperation Operation = "DELETE"
)

type WALRecord struct {
	Operation Operation
	Key []byte
	Value []byte
}

func check(e error) {
    if e != nil {
        panic(e)
    }
}

type WriteAheadLog struct {
	file         *os.File
	path         string
	bufferWriter *bufio.Writer
	gobEncoder   *gob.Encoder
}

func New(path string) *WriteAheadLog {
	
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	check(err)

	writer := bufio.NewWriter(f)

	encoder := gob.NewEncoder(writer)

	return &WriteAheadLog{
		file: f,
		path: path,
		bufferWriter: writer,
		gobEncoder: encoder,
	}

}

func (w *WriteAheadLog) Write(record WALRecord) {
	err := w.gobEncoder.Encode(record)

	check(err)

	w.bufferWriter.Flush()

}

func (w *WriteAheadLog) Recover() []WALRecord {

	f, err := os.OpenFile(w.path, os.O_RDONLY, 0644)
	check(err)

	buffer := bufio.NewReader(f)
	decoder := gob.NewDecoder(buffer)

	record := WALRecord{}
	records := []WALRecord{}

	for {
		err := decoder.Decode(&record)

		if err == io.EOF {
			break
		} else if err != nil {
			check(err)
		} else {
			records = append(records, record)
		}
	}

	return records
}