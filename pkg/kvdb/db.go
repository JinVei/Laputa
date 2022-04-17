package kvdb

import (
	"Laputa/pkg/kvdb/common"
	"Laputa/pkg/kvdb/log"
	"Laputa/pkg/kvdb/memtable"
	"Laputa/pkg/kvdb/table"
	"Laputa/pkg/kvdb/versionset"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
)

type DB struct {
	opts     *common.Options
	journal  *log.Writer
	fjournal *os.File
	vset     *versionset.VersionSet
	mtable   *memtable.Memtable
	immtable *memtable.Memtable

	mtableLock *sync.RWMutex
	logLock    *sync.Mutex

	maybeCompactC chan struct{}
	closeC        chan struct{}
	tCache        *TableCache

	blockImmCond *sync.Cond
}

func NewDB(opts *common.Options) *DB {
	db := new(DB)
	db.opts = opts
	db.vset = versionset.NewVersionSet(opts)
	db.mtable = memtable.New(opts.KeyComparator)
	//db.mtable.Ref()
	db.mtableLock = &sync.RWMutex{}
	db.logLock = &sync.Mutex{}
	db.maybeCompactC = make(chan struct{}, 1)
	db.closeC = make(chan struct{}, 1)

	db.blockImmCond = sync.NewCond(db.mtableLock)
	db.tCache = NewTableCache(opts)

	return db
}

func (db *DB) Recover() error {
	err := db.vset.Recover()
	if err != nil {
		return err
	}
	err = db.recoverJournal()

	go db.comapctCoroutine()
	db.initSizeCompact()

	return err
}

func (db *DB) Get(key []byte) ([]byte, bool) {
	db.mtableLock.RLock()

	lkey := memtable.LookupKey{}

	lkey.Sequence = db.vset.LastSequence()
	lkey.Key = key
	//db.mtable.Ref()
	vallue, exist := db.mtable.Get(lkey)
	//db.mtable.Unref()

	if !exist && db.immtable != nil {
		//db.immtable.Ref()
		vallue, exist = db.immtable.Get(lkey)
		//db.immtable.Unref()
	}
	db.mtableLock.RUnlock()

	if !exist {
		return db.findKVFromSST(key)
	}

	return vallue, exist
}

func (db *DB) findKVFromSST(key []byte) ([]byte, bool) {
	vers := db.vset.RefVersion()
	defer vers.Unref()
	for level, metas := range vers.MetaFiles {
		for _, meta := range metas {
			if db.opts.KeyComparator(key, meta.Smallest.UserKey()) < 0 {
				break
			}

			if db.opts.KeyComparator(meta.Largest.UserKey(), key) < 0 {
				continue
			}

			sst, err := db.tCache.Get(meta.Number)
			if err != nil {
				fmt.Println("db.tCache.Get(meta.Number)", err)
			}
			sst.Seek(key)
			meta.AllowedSeeks++
			if db.opts.MaxAllowSeek < meta.AllowedSeeks {
				db.vset.SetSeekCompact(level, meta)
				db.signalForDoCompact()
			}
			if sst.Valid() {
				return sst.Value(), true
			}
		}
	}
	return nil, false
}

func (db *DB) Put(key, value []byte) error {
	db.mtableLock.Lock()
	if err := db.checkMemtableUsage(); err != nil {
		db.mtableLock.Unlock()
		return err
	}
	db.mtableLock.Unlock()

	sequence := db.vset.NextSequence()
	entry := memtable.NewEntry(sequence, key, value, memtable.KTypeValue)

	db.logLock.Lock()
	err := db.journal.AddRecord(entry.Bytes())
	if err != nil {
		db.logLock.Unlock()
		return err
	}
	db.logLock.Unlock()

	db.mtableLock.Lock()
	defer db.mtableLock.Unlock()
	db.mtable.PutEntry(entry)

	return nil
}

func (db *DB) Delete(key []byte) error {
	db.mtableLock.Lock()
	if err := db.checkMemtableUsage(); err != nil {
		db.mtableLock.Unlock()
		return err
	}
	db.mtableLock.Unlock()

	sequence := db.vset.NextSequence()
	entry := memtable.NewEntry(sequence, key, nil, memtable.KTypeDelete)

	db.logLock.Lock()
	err := db.journal.AddRecord(entry.Bytes())
	if err != nil {
		db.logLock.Unlock()
		return err
	}
	db.logLock.Unlock()

	db.mtableLock.Lock()
	defer db.mtableLock.Unlock()
	db.mtable.PutEntry(entry)

	return nil
}

var done = false

// should lock db.mtableLock
func (db *DB) checkMemtableUsage() error {
	// minor compact
	if db.opts.MaxMemtableSize < db.mtable.MemoryUsage() {
		for db.immtable != nil {
			db.blockImmCond.Wait()
		}

		// return from db.blockWriteCond.Wait() may had flush memtable
		// so should check again for whether it needs flush memtable
		if db.opts.MaxMemtableSize < db.mtable.MemoryUsage() {

			atomic.StoreUint64(&db.vset.PrevLogNumber, db.vset.LogNumber)
			db.vset.LogNumber = db.vset.NextFileNumber()

			logPath := filepath.Join(db.opts.DBDir, common.GetLogName(db.vset.LogNumber))
			wlog, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0766)
			if err != nil {
				return err
			}
			db.fjournal.Close()
			db.fjournal = wlog
			db.journal = log.NewWriter(wlog)

			db.immtable = db.mtable

			db.mtable = memtable.New(db.opts.KeyComparator)

			db.signalForDoCompact()
		}
	}
	return nil
}

func (db *DB) signalForDoCompact() {
	// signal for should do compact
	select {
	case db.maybeCompactC <- struct{}{}:
	default:
		// For blockless purpose
	}
}

func (db *DB) doMinorCompact(mtable *memtable.Memtable, fileNumber, oldLognumber uint64) error {
	iter := mtable.Iterator()
	var last []byte
	if !iter.Valid() {
		// empty log
		err := os.Remove(filepath.Join(db.opts.DBDir, common.GetLogName(oldLognumber)))
		if err != nil {
			fmt.Println("Get error in os.Remove:", err)
		}
		return nil
	}
	firstKey := append([]byte{}, iter.Get().InternalKey()...)

	tname := filepath.Join(db.opts.DBDir, common.GetTableName(fileNumber))

	ftable, err := os.OpenFile(tname, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0766)
	if err != nil {
		return err
	}
	defer ftable.Close()

	tbuidler := table.NewTableBuilder(ftable, db.opts)

	for ; iter.Valid(); iter.Next() {
		entry := iter.Get()
		if last == nil {
			last = append(last, entry.Bytes()...)
			continue
		}

		lastEntry := memtable.DecodeEntry(last)
		if db.opts.KeyComparator(lastEntry.UserKey(), entry.UserKey()) == 0 && entry.Sequence() <= lastEntry.Sequence() {
			continue
		}

		tbuidler.Add(entry.InternalKey(), entry.Value())
		last = append(last[0:0], entry.Bytes()...)
	}
	err = tbuidler.Finish()
	if err != nil {
		return err
	}

	stat, _ := ftable.Stat()

	meta := common.NewFileMetaData(fileNumber, uint64(stat.Size()), firstKey, memtable.DecodeEntry(last).InternalKey())
	meta.Refs++

	ve := versionset.VersionEdit{}
	ve.AddFile(0, meta)
	db.vset.LogAndApply(&ve)

	currVers := db.vset.CurrentVersion() //
	if db.opts.L0CompactTriggerNum <= len(currVers.MetaFiles[0]) {
		currVers.CompactionScore = float64(len(currVers.MetaFiles[0])) / float64(db.opts.L0CompactTriggerNum)
		currVers.CompactionLevel = 0
	}

	err = os.Remove(filepath.Join(db.opts.DBDir, common.GetLogName(oldLognumber)))
	if err != nil {
		fmt.Println("Get error in os.Remove:", err)
	}

	return err
}

func (db *DB) listLogNumbers() ([]int, error) {
	dirs, err := os.ReadDir(db.opts.DBDir)
	if err != nil {
		return nil, err
	}
	numbers := make([]int, 0, len(dirs))
	for _, dir := range dirs {
		if common.IsLogFile(dir.Name()) {
			numbers = append(numbers, int(common.LogNameToNumber(dir.Name())))
		}
	}
	sort.Ints(numbers)
	return numbers, nil
}

func (db *DB) recoverJournal() error {
	logNumbers, err := db.listLogNumbers()
	if err != nil {
		return err
	}
	reuseLastLog := false
	i, number := 0, 0
	for i, number = range logNumbers {
		logPath := filepath.Join(db.opts.DBDir, common.GetLogName(uint64(number)))
		rlog, err := os.Open(logPath)
		if err != nil {
			return err
		}
		defer rlog.Close()

		rjournal := log.NewReader(rlog)
		for {
			record, err := rjournal.ReadRecord()
			if err != nil && err != io.EOF {
				return err
			}
			if err == io.EOF {
				break
			}
			entry := memtable.DecodeEntry(record)
			newentry := memtable.NewEntry(entry.Sequence(), entry.UserKey(), entry.Value(), entry.ValueType())
			db.mtable.PutEntry(newentry)
			if db.vset.LastSequence() < entry.Sequence() {
				db.vset.ResetSequence(entry.Sequence())
			}
		}

		if i < len(logNumbers) {
			if i == len(logNumbers)-1 && db.mtable.MemoryUsage() < db.opts.MaxMemtableSize {
				reuseLastLog = true
				continue
			}
			if err := db.doMinorCompact(db.mtable, db.vset.NextFileNumber(), uint64(number)); err != nil {
				return err
			}
			db.mtable = memtable.New(db.opts.KeyComparator)
		}
	}
	lognumbber := uint64(number)
	if !reuseLastLog {
		lognumbber = db.vset.NextFileNumber()
	}

	logPath := filepath.Join(db.opts.DBDir, common.GetLogName(lognumbber))
	wlog, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0766)
	if err != nil {
		return err
	}
	db.fjournal = wlog
	db.journal = log.NewWriter(wlog)
	ve := new(versionset.VersionEdit)

	db.vset.LogNumber = lognumbber
	ve.SetLogNumber(lognumbber)
	db.vset.LogAndApply(ve)
	return nil
}

func (db *DB) comapctCoroutine() {
	for {
		select {
		case <-db.maybeCompactC:
			db.DoComapct()
		case <-db.closeC:
			return
		}
	}
}

func (db *DB) DoComapct() {
	for {
		var immtable *memtable.Memtable
		immLogNumber := uint64(0)
		db.mtableLock.Lock()
		if db.immtable != nil {
			immtable = db.immtable
			db.immtable = nil
			immLogNumber = db.vset.PrevLogNumber
		}
		db.mtableLock.Unlock()

		if immtable != nil {
			err := db.doMinorCompact(immtable, db.vset.NextFileNumber(), immLogNumber)
			if err != nil {
				fmt.Println("DoComapct:", err)
			}
			immtable = nil
			db.blockImmCond.Broadcast()

		} else if db.vset.NeedCompact() {
			compact := db.vset.PickCompaction()
			db.doMajorCompact(compact)
		} else {
			break
		}
	}

}

func (db *DB) Close() {
	close(db.closeC)
}

func (db *DB) doMajorCompact(compact *versionset.Compaction) {
	if compact == nil {
		return
	}

	iters := make([]*table.Iterator, 0, len(compact.Inputs[0])+len(compact.Inputs[1]))
	for _, in := range compact.Inputs {
		for _, meta := range in {
			iter, err := db.tCache.Get(meta.Number)
			if err != nil {
				fmt.Println(err)
				continue
			}
			iters = append(iters, iter)
		}
	}

	nextLevel := compact.Level + 1
	tableSize := db.vset.LevelTableSize(nextLevel)
	miter := table.NewMergingIterator(iters, db.opts.InternalKeyCompare)
	metas := make([]*common.FileMetaData, 0, 1)

	var first common.InternalKey
	var last common.InternalKey

	for {
		tnumber := db.vset.NextFileNumber()
		sstname := filepath.Join(db.opts.DBDir, common.GetTableName(tnumber))
		f, err := os.OpenFile(sstname, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0766)
		if err != nil {
			// TODO
			panic(err)
		}
		defer f.Close()
		tbuilder := table.NewTableBuilder(f, db.opts)
		if first != nil {
			first = first[0:0]
		}
		first = append(first, miter.Key()...)

		for ; miter.Valid(); miter.Next() {
			ikey := common.InternalKey(miter.Key())

			// if current sstable too large, we should create new sstable
			// any equal userkey record should layin same sstable
			if tableSize < uint64(tbuilder.EstimatedSize()) &&
				last != nil && db.opts.KeyComparator(ikey.UserKey(), last.UserKey()) != 0 {
				break
			}
			if last != nil &&
				db.opts.KeyComparator(ikey.UserKey(), last.UserKey()) == 0 &&
				ikey.Sequence() <= last.Sequence() {
				continue
			}
			err := tbuilder.Add(miter.Key(), miter.Value())
			if err != nil {
				// TODO
				panic(err)
			}
			if last != nil {
				last = last[0:0]
			}
			last = append(last, miter.Key()...)

		}

		tbuilder.Finish()
		meta := common.NewFileMetaData(tnumber, uint64(tbuilder.EstimatedSize()), first, last)
		metas = append(metas, meta)
		if !miter.Valid() {
			break
		}
	}
	db.vset.CompactPointer[nextLevel] = last
	db.vset.CompactPointer[compact.Level] = last

	ve := versionset.VersionEdit{}
	for _, meta := range metas {
		ve.AddFile(nextLevel, meta)
	}
	//delete
	for i, in := range compact.Inputs {
		for _, meta := range in {
			ve.DelFile(compact.Level+i, meta.Number)
		}
	}

	ve.SetNextFileNumber(db.vset.GetLastFileNumber())
	if db.vset.CurrentVersion().LastSequence != db.vset.LastSequence() {
		ve.SetNextFileNumber(db.vset.LastSequence())
	}

	db.vset.LogAndApply(&ve)

	// vers := db.vset.CurrentVersion()
	// levelFileSize := 0
	// for _, meta := range vers.MetaFiles[nextLevel] {
	// 	levelFileSize += int(meta.FileSize)
	// }
	// initSizeCompact
	db.initSizeCompact()
	// compactscore := float64(levelFileSize) / float64(db.vset.LevelTableSize(nextLevel))
	// vers.CompactionScore = compactscore
	// vers.CompactionLevel = nextLevel
}

func (db *DB) initSizeCompact() {
	vers := db.vset.CurrentVersion()
	for level, metas := range vers.MetaFiles {
		if level == 0 {
			if db.opts.L0CompactTriggerNum < len(vers.MetaFiles[0]) {
				vers.CompactionScore = float64(len(vers.MetaFiles[0])) / float64(db.opts.L0CompactTriggerNum)
				vers.CompactionLevel = 0
				db.signalForDoCompact()
				return
			}
		} else {
			levelFileSize := 0
			for _, meta := range metas {
				levelFileSize += int(meta.FileSize)
			}
			compactscore := float64(levelFileSize) / float64(db.vset.LevelTableSize(level))
			vers.CompactionScore = compactscore
			vers.CompactionLevel = level
			if 1 <= vers.CompactionScore {
				db.signalForDoCompact()
				return
			}
		}
	}
}

// TODO
// clean unuse sstable
// LRU sstable Cache
// filter block
