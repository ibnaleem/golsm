package sstable

import (
	"time"

	"github.com/bits-and-blooms/bloom"
)

type SSTable struct {
	path       string
	timestamp  time.Time
}