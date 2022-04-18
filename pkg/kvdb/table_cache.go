package kvdb

import (
	"Laputa/pkg/kvdb/common"
	"Laputa/pkg/kvdb/table"
	"os"
	"path/filepath"
	"sync"
)

// sstable cache
// TODO: LRC
type TableCache struct {
	opts  *common.Options
	cache map[uint64]*table.Table
	lock  sync.RWMutex
}

func NewTableCache(opts *common.Options) *TableCache {
	tc := new(TableCache)
	tc.cache = make(map[uint64]*table.Table)
	tc.opts = opts
	return tc
}

// Get sstbale from cache by given sstable number
func (tc *TableCache) Get(number uint64) (*table.Iterator, error) {
	tc.lock.RLock()
	defer tc.lock.RUnlock()

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

// Delete remove sstbale from cache by given sstable number
func (tc *TableCache) Delete(number uint64) {
	tc.lock.Lock()
	defer tc.lock.Unlock()

	t, exist := tc.cache[number]
	if exist {
		f := t.GetWritedFile()
		f.Close()
	}
}
