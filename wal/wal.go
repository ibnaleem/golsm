package wal

import (
	"os"
	"bufio"
	"encoding/gob"
)

type WriteAheadLog struct {
	file         *os.File
	bufferWriter *bufio.Writer
	gobEncoder   *gob.Encoder
}