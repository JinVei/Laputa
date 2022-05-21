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

	"github.com/juju/fslock"
)

type DB struct {
	opts     *common.Options
	journal  *log.Writer
	fjournal *os.File
	vset     *versionset.VersionSet
	mtable   *memtable.Memtable
	immtable *memtable.Memtable

	mtableLock  *sync.RWMutex
	journalLock *sync.Mutex

	maybeCompactC chan struct{}
	exitCompactC  chan struct{}
	closeC        chan struct{}
	tCache        *TableCache
	log           *os.File
	dbLock        *fslock.Lock

	blockImmCond *sync.Cond
}

func NewDB(opts *common.Options) *DB {
	db := new(DB)
	db.opts = opts
	db.vset = versionset.NewVersionSet(opts)
	db.mtable = memtable.New(opts.KeyComparator)
	db.mtableLock = &sync.RWMutex{}
	db.journalLock = &sync.Mutex{}
	db.maybeCompactC = make(chan struct{}, 1)
	db.exitCompactC = make(chan struct{}, 1)
	db.closeC = make(chan struct{}, 1)

	db.blockImmCond = sync.NewCond(db.mtableLock)
	db.tCache = NewTableCache(opts)

	db.dbLock = fslock.New(filepath.Join(db.opts.DBDir, common.GetDBLockerName()))

	return db
}

// Recover try to recover last version from manifest and memtable from logfile
func (db *DB) Recover() error {

	if err := os.MkdirAll(db.opts.DBDir, 0766); err != nil && err != os.ErrExist {
		return err
	}

	if err := db.dbLock.TryLock(); err != nil {
		return err
	}

	err := db.vset.Recover()
	if err != nil {
		return err
	}
	err = db.recoverJournal()
	if err != nil {
		return err
	}

	go db.comapctCoroutine()
	db.updateSizeCompactPointer()

	db.log, err = os.OpenFile(filepath.Join(db.opts.DBDir, common.GetLogName()), os.O_CREATE|os.O_TRUNC|os.O_WRONLY|os.O_APPEND, 0766)

	return err
}

func (db *DB) LOG(msg string) {
	db.log.Write([]byte(msg + "\n"))
}

// Get seek key and return its value
func (db *DB) Get(key []byte) ([]byte, bool) {
	db.mtableLock.RLock()

	lkey := memtable.LookupKey{}

	lkey.Sequence = db.vset.LastSequence()
	lkey.Key = key
	vallue, exist := db.mtable.Get(lkey)

	if !exist && db.immtable != nil {
		vallue, exist = db.immtable.Get(lkey)
	}
	db.mtableLock.RUnlock()

	if !exist {
		return db.findKVFromSST(key)
	}

	return vallue, exist
}

// FindKVFromSST seeks key from sstables and returns its value (if existed)
func (db *DB) findKVFromSST(key []byte) ([]byte, bool) {
	vers := db.vset.RefVersion()
	defer vers.Unref()
	for level, metas := range vers.MetaFiles {
		for _, meta := range metas {
			if db.opts.KeyComparator(key, meta.Smallest.UserKey()) < 0 {
				if level != 0 {
					// SST is sorted except in level 0
					break
				} else {
					continue
				}
			}

			if db.opts.KeyComparator(meta.Largest.UserKey(), key) < 0 {
				continue
			}

			sst, err := db.tCache.Get(meta.Number)
			if err != nil {
				db.LOG(fmt.Sprintf("FindKVFromSST: Get error from sstable cache, err=%v", err))
			}
			sst.Seek(key)
			meta.AllowedSeeks++
			if level < 11 && db.opts.MaxAllowSeek < meta.AllowedSeeks {
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

// Put put Key to db
func (db *DB) Put(key, value []byte) error {
	db.mtableLock.Lock()
	if err := db.checkMemtableUsage(); err != nil {
		db.mtableLock.Unlock()
		return err
	}
	db.mtableLock.Unlock()

	sequence := db.vset.NextSequence()
	entry := memtable.NewEntry(sequence, key, value, memtable.KTypeValue)

	db.journalLock.Lock()
	err := db.journal.AddRecord(entry.Bytes())
	if err != nil {
		db.journalLock.Unlock()
		return err
	}
	if db.opts.SyncWrites {
		db.journal.Flush()
	}
	db.journalLock.Unlock()

	db.mtableLock.Lock()
	defer db.mtableLock.Unlock()
	db.mtable.PutEntry(entry)

	return nil
}

// Delete delete Key from db
func (db *DB) Delete(key []byte) error {
	db.mtableLock.Lock()
	if err := db.checkMemtableUsage(); err != nil {
		db.mtableLock.Unlock()
		return err
	}
	db.mtableLock.Unlock()

	sequence := db.vset.NextSequence()
	entry := memtable.NewEntry(sequence, key, nil, memtable.KTypeDelete)

	db.journalLock.Lock()
	err := db.journal.AddRecord(entry.Bytes())
	if err != nil {
		db.journalLock.Unlock()
		return err
	}
	db.journalLock.Unlock()

	db.mtableLock.Lock()
	defer db.mtableLock.Unlock()
	db.mtable.PutEntry(entry)

	return nil
}

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

			logPath := filepath.Join(db.opts.DBDir, common.GetJoutnalName(db.vset.LogNumber))
			wlog, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0766)
			if err != nil {
				return err
			}
			db.fjournal.Close()
			db.fjournal = wlog
			db.journal = log.NewWriter(wlog)

			db.LOG(fmt.Sprintf("New journal: number=%d", db.vset.LogNumber))

			db.immtable = db.mtable

			db.mtable = memtable.New(db.opts.KeyComparator)

			db.signalForDoCompact()
		}
	}
	return nil
}

// SignalForDoCompact try to awake Compat Coroutine
func (db *DB) signalForDoCompact() {
	// signal for should do compact
	select {
	case db.maybeCompactC <- struct{}{}:
	default:
		// For blockless purpose
	}
}

// DoMinorCompact flush memtable into sstable, and delete its logfile
func (db *DB) doMinorCompact(mtable *memtable.Memtable, fileNumber, oldLognumber uint64) error {
	db.LOG(fmt.Sprintf("Do minor compact: sst_number=%d, old_journal=%d", fileNumber, oldLognumber))
	iter := mtable.Iterator()
	var last []byte
	if !iter.Valid() {
		// empty log
		err := os.Remove(filepath.Join(db.opts.DBDir, common.GetJoutnalName(oldLognumber)))
		if err != nil {
			db.LOG(fmt.Sprintf("Do minor compact: os.remove err=%v", err))
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
			tbuidler.Add(entry.InternalKey(), entry.Value())
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

	db.updateSizeCompactPointer()

	err = os.Remove(filepath.Join(db.opts.DBDir, common.GetJoutnalName(oldLognumber)))
	if err != nil {
		db.LOG(fmt.Sprintf("Do minor compact: remove journal err, journal_number=%d", oldLognumber))
	}
	db.LOG(fmt.Sprintf("Do minor compact: Done compact. sst_number=%d, old_journal=%d", fileNumber, oldLognumber))

	db.cleanUnrefVersion()

	return err
}

// ListLogNumbers list all logfile's number, and sort by descend
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
		logPath := filepath.Join(db.opts.DBDir, common.GetJoutnalName(uint64(number)))
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

	logPath := filepath.Join(db.opts.DBDir, common.GetJoutnalName(lognumbber))
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
			db.LOG("Comapct Coroutine: exit")
			close(db.exitCompactC)
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
				db.LOG(fmt.Sprintf("Do Comapct: Get error from doMinorCompact, err=%v", err))
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
	<-db.exitCompactC
	db.fjournal.Close()
	db.fjournal = nil
	db.cleanUnrefVersion()
	db.log.Close()
	db.dbLock.Unlock()
}

func (db *DB) doMajorCompact(compact *versionset.Compaction) error {
	if compact == nil {
		return nil
	}

	db.LOG(fmt.Sprintf("Do Major Compact: level=%d, ", compact.Level))

	iters := make([]*table.Iterator, 0, len(compact.Inputs[0])+len(compact.Inputs[1]))
	for _, in := range compact.Inputs {
		for _, meta := range in {
			iter, err := db.tCache.Get(meta.Number)
			if err != nil {
				db.LOG(fmt.Sprintf("Do Major Compact: Get error from sstable cache, err=%v", err))
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
		first = nil
		last = nil
		metas = append(metas, meta)
		if !miter.Valid() {
			break
		}
	}
	db.vset.CompactPointer[nextLevel] = last
	db.vset.CompactPointer[compact.Level] = last

	ve := versionset.VersionEdit{}
	for i := 0; i < len(metas); i++ {
		db.LOG(fmt.Sprintf("Do Major Compact: add new sst to version. level=%d, sst=%d ", compact.Level, metas[i].Number))
		ve.AddFile(nextLevel, metas[i])
	}
	//delete
	for i, in := range compact.Inputs {
		for _, meta := range in {
			db.LOG(fmt.Sprintf("Do Major Compact: remove sst from version. level=%d, sst=%d ", compact.Level, meta.Number))
			ve.DelFile(compact.Level+i, meta.Number)
		}
	}

	ve.SetNextFileNumber(db.vset.GetLastFileNumber())
	if db.vset.CurrentVersion().LastSequence != db.vset.LastSequence() {
		ve.SetNextFileNumber(db.vset.LastSequence())
	}

	db.vset.LogAndApply(&ve)

	db.updateSizeCompactPointer()

	db.cleanUnrefVersion()
	return nil
}

func (db *DB) updateSizeCompactPointer() {
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

func (db *DB) cleanUnrefVersion() {
	unrefVers := db.vset.RemoveUnrefVersion()
	vers := db.vset.GetRefVersions()

	refMetas := make(map[uint64]*common.FileMetaData)
	for _, ver := range vers {
		for _, levelMetas := range ver.MetaFiles {
			for _, meta := range levelMetas {
				refMetas[meta.Number] = meta
			}
		}
	}

	var unrefMetas []*common.FileMetaData
	for _, v := range unrefVers {
		db.LOG(fmt.Sprintf("Clean Unref Version: Seq=%d", v.Seq))
		for _, metas := range v.MetaFiles {
			for _, meta := range metas {
				if _, existed := refMetas[meta.Number]; !existed {
					unrefMetas = append(unrefMetas, meta)
				}
			}
		}
	}

	// delete unref sstable
	for _, meta := range unrefMetas {
		db.tCache.Delete(meta.Number)
		sstPath := filepath.Join(db.opts.DBDir, common.GetTableName(meta.Number))
		os.Remove(sstPath)
		db.LOG(fmt.Sprintf("Clean Unref Version sst: sst_path=%s", sstPath))
	}
}

// TODO
// LRU sstable Cache
// filter block
// batch write
