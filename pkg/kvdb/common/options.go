package common

import "bytes"

type Options struct {
	KeyComparator        Compare
	InternalKeyCompare   Compare
	TableBlockSize       int // byte
	RestartInterval      int
	IndexRestartInterval int
	CompressTableBlock   bool
	DBDir                string
	MaxMemtableSize      uint64 // byte
	L0TableSize          uint64
	MaxAllowSeek         int
	L0CompactTriggerNum  int
}

func NewDefaultOptions() *Options {
	opts := &Options{}
	opts.KeyComparator = bytes.Compare
	opts.InternalKeyCompare = NewInternalKeyCompare(opts.KeyComparator).Compare
	opts.TableBlockSize = 1 * 1024
	opts.RestartInterval = 20
	opts.IndexRestartInterval = 5
	opts.DBDir = "./db"
	opts.MaxMemtableSize = 1024 * 1024 // 1M
	opts.L0CompactTriggerNum = 4
	opts.L0TableSize = 1024 * 1024 * 4 // 4M
	opts.MaxAllowSeek = 100            // TODO
	return opts
}
