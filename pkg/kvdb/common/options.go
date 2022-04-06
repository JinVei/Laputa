package common

import "bytes"

type Options struct {
	KeyComparator      Compare
	TableBlockSize     int
	RestartInterval    int
	CompressTableBlock bool
}

func NewDefaultOptions() *Options {
	opts := &Options{}
	opts.KeyComparator = bytes.Compare
	opts.TableBlockSize = 1 * 1024
	opts.RestartInterval = 20
	return opts
}