package wal

import (
	"os"
	"bufio"
	"encoding/gob"
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
		bufferWriter: writer,
		gobEncoder: encoder,
	}

}

func (w *WriteAheadLog) Write(record WALRecord) {
	err := w.gobEncoder.Encode(record)

	check(err)

	w.bufferWriter.Flush()

}