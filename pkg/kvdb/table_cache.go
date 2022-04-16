package kvdb

import (
	"Laputa/pkg/kvdb/common"
	"Laputa/pkg/kvdb/table"
	"os"
	"path/filepath"
)

// TODO: LRC
type TableCache struct {
	opts  *common.Options
	cache map[uint64]*table.Table
}

func NewTableCache(opts *common.Options) *TableCache {
	tc := new(TableCache)
	tc.cache = make(map[uint64]*table.Table)
	tc.opts = opts
	return tc
}

func (tc *TableCache) Get(number uint64) (*table.Iterator, error) {
	t, exist := tc.cache[number]
	if !exist {
		f, err := os.Open(filepath.Join(tc.opts.DBDir, common.GetTableName(number)))
		if err != nil {
			return nil, err
		}
		t = table.New(tc.opts)
		if err := t.Open(f); err != nil {
			f.Close()
			return nil, err
		}
	}

	tc.cache[number] = t
	return t.NewIterator(), nil
}
