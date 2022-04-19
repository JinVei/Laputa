package versionset

import (
	"Laputa/pkg/kvdb/common"
	"Laputa/pkg/kvdb/log"
	"bytes"
	"errors"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
)

var (
	ErrUnknowVersionEditTag = errors.New("ErrUnknowVersionEditTag")
)

type Version struct {
	Comparator     string
	LogNumber      uint64
	NextFileNumber uint64
	LastSequence   uint64
	MetaFiles      [common.MaxLevels][]*common.FileMetaData
	compactPointer *LevelInternalKeyPair

	Next *Version
	prev *Version

	CompactionScore float64
	CompactionLevel int

	FileToCompact      *common.FileMetaData
	FileToCompactLevel int

	vset *VersionSet
	ref  int64
}

type Compaction struct {
	Level        int
	Inputs       [2][]*common.FileMetaData
	InputVersion *Version
}

type VersionSet struct {
	manifestNumber uint64
	current        *Version
	dbname         string

	lastFileNumber uint64
	lastSequence   uint64
	LogNumber      uint64
	PrevLogNumber  uint64
	CompactPointer [common.MaxLevels]common.InternalKey

	//Lock         *sync.RWMutex
	dummyVersion Version

	manifest   *log.Writer
	KeyCompare common.Compare
	opts       *common.Options
	versLock   *sync.RWMutex
	//UserKeyCompare common.Compare

}

func NewVersionSet(opts *common.Options) *VersionSet {
	vs := new(VersionSet)
	vs.dbname = opts.DBDir
	//vs.Lock = &sync.RWMutex{}
	vs.opts = opts
	vs.KeyCompare = opts.KeyComparator
	vs.versLock = &sync.RWMutex{}

	return vs
}

func NewCompaction(opts *common.Options) *Compaction {
	c := &Compaction{}
	return c
}

func (vs *VersionSet) NeedCompact() bool {
	return 1 <= vs.current.CompactionScore || vs.current.FileToCompact != nil
}

func (vs *VersionSet) PickCompaction() *Compaction {
	sizeCompacttion := 1 <= vs.current.CompactionScore
	seekCompacttion := vs.current.FileToCompact != nil

	level := 0
	c := NewCompaction(vs.opts)
	if sizeCompacttion {
		level = vs.current.CompactionLevel
		c.Level = level
		metaFiles := vs.current.MetaFiles[level]
		for _, f := range metaFiles {
			if vs.CompactPointer[level] == nil ||
				vs.KeyCompare(f.Largest, vs.CompactPointer[level]) <= 0 {
				c.Inputs[0] = append(c.Inputs[0], f)
				break
			}
		}
		if len(c.Inputs[0]) == 0 {
			c.Inputs[0] = append(c.Inputs[0], metaFiles[0])
		}

	} else if seekCompacttion {
		level = vs.current.CompactionLevel
		c.Level = level
		c.Inputs[0] = append(c.Inputs[0], vs.current.FileToCompact)
	} else {
		return nil
	}

	c.InputVersion = vs.current
	c.InputVersion.Ref()
	defer c.InputVersion.Unref()

	smallest, largest := vs.GetRange(c.Inputs[0])
	smallest, largest, c.Inputs[0] = vs.current.GetOverlappingInputs(level, smallest, largest, c.Inputs[0])

	smallest, largest, c.Inputs[1] = vs.current.GetOverlappingInputs(level+1, smallest, largest, c.Inputs[1])

	vs.CompactPointer[level] = largest
	return c
}

func (vs *VersionSet) LogAndApply(edit *VersionEdit) {
	newVersion := NewVersion(vs, vs.current, edit)
	record := bytes.NewBuffer(make([]byte, 0))
	edit.EncodeTo(record)
	vs.manifest.AddRecord(record.Bytes())

	vs.current.prev = &vs.dummyVersion
	vs.current.Next = vs.dummyVersion.Next
	vs.dummyVersion.Next = vs.current
	vs.current.Unref()
	vs.setVersion(newVersion)
	newVersion.Ref()
}

func (vs *VersionSet) Recover() error {
	current := filepath.Join(vs.dbname, common.GetCurrentName())
	fcurrent, err := os.OpenFile(current, os.O_CREATE|os.O_RDWR, 0766)
	if err != nil {
		return err
	}
	defer fcurrent.Close()

	manifestNum, err := ioutil.ReadAll(fcurrent)
	if err != nil && err != io.EOF {
		return err
	}
	ve := new(VersionEdit)
	oldmanifestPath := filepath.Join(vs.dbname, common.GetManifestNameBytes(manifestNum))
	if string(manifestNum) == "" {
		// may be new database
		goto END_READ_MANIFEST
	}

	{
		fmanifest, err := os.OpenFile(oldmanifestPath, os.O_CREATE|os.O_RDONLY, 0766)
		if err != nil {
			return err
		}
		defer fmanifest.Close()
		logReader := log.NewReader(fmanifest)
		for {
			record, err := logReader.ReadRecord()
			if err != nil && err != io.EOF {
				return err
			}
			if err == io.EOF {
				goto END_READ_MANIFEST
			}

			v := VersionEdit{}
			if err := v.DecodeFrom(record); err != nil {
				return err
			}
			ve.Apply(v)
		}
	}
END_READ_MANIFEST:

	err = vs.recoverNextFileNumber()
	if err != nil {
		return err
	}

	newManifestNum := vs.NextFileNumber()
	newmanifest := filepath.Join(vs.dbname, common.GetManifestName(newManifestNum))
	fmanifest, err := os.OpenFile(newmanifest, os.O_CREATE|os.O_TRUNC|os.O_WRONLY|os.O_APPEND, 0766)
	if err != nil {
		return err
	}
	vs.manifest = log.NewWriter(fmanifest)

	ve.SetNextFileNumber(vs.lastFileNumber)

	version := NewVersion(vs, nil, ve)
	version.Ref()
	vs.lastSequence = version.LastSequence
	vs.LogNumber = version.LogNumber
	vs.current = version

	record := bytes.NewBuffer(make([]byte, 0))
	ve.EncodeTo(record)
	vs.manifest.AddRecord(record.Bytes())

	if _, err := fcurrent.WriteAt([]byte(strconv.Itoa(int(newManifestNum))), 0); err != nil {
		return err
	}

	fcurrent.Sync()
	os.Remove(oldmanifestPath)

	return nil
}

func (vs *VersionSet) GetRange(inputs []*common.FileMetaData) (smallest, largest common.InternalKey) {
	for i, in := range inputs {
		if i == 0 {
			largest = in.Largest
			smallest = in.Smallest
		}
		if vs.KeyCompare(in.Smallest, smallest) < 0 {
			smallest = in.Smallest
		}
		if 0 < vs.KeyCompare(in.Largest, largest) {
			largest = in.Largest
		}
	}
	return
}

func (vs *VersionSet) NextSequence() uint64 {
	return atomic.AddUint64(&vs.lastSequence, 1)
}

func (vs *VersionSet) LastSequence() uint64 {
	return atomic.LoadUint64(&vs.lastSequence)
}

func (vs *VersionSet) ResetSequence(sequence uint64) {
	atomic.StoreUint64(&vs.lastSequence, sequence)
}

func (vs *VersionSet) NextFileNumber() uint64 {
	return atomic.AddUint64(&vs.lastFileNumber, 1)
}

func (vs *VersionSet) GetLastFileNumber() uint64 {
	return atomic.LoadUint64(&vs.lastFileNumber)
}

func (v *Version) Ref() { v.ref++ }
func (v *Version) Unref() {
	v.ref--
}

func (v *Version) GetOverlappingInputs(level int, smallest, largest common.InternalKey, inputs []*common.FileMetaData) (common.InternalKey, common.InternalKey, []*common.FileMetaData) {
	if inputs == nil {
		inputs = make([]*common.FileMetaData, 0)
	}
	inputs = inputs[0:0]
	for i := 0; i < len(v.MetaFiles[level]); {
		meta := v.MetaFiles[level][i]
		i++
		if smallest != nil && v.vset.KeyCompare(meta.Largest, smallest) < 0 {

		} else if largest != nil && v.vset.KeyCompare(largest, meta.Smallest) < 0 {

		} else {
			inputs = append(inputs, meta)
			if smallest != nil && v.vset.KeyCompare(meta.Smallest, smallest) < 0 {
				smallest = meta.Smallest
				inputs = inputs[0:0]
				i = 0
			} else if largest != nil && v.vset.KeyCompare(largest, meta.Largest) < 0 {
				largest = meta.Largest
				inputs = inputs[0:0]
				i = 0
			}
		}
	}
	return smallest, largest, inputs
}

func (vs *VersionSet) recoverNextFileNumber() error {
	dirs, err := os.ReadDir(vs.dbname)
	if err != nil {
		return err
	}
	for _, dir := range dirs {
		number := common.ParseFileNameNumber(dir.Name())
		if number < 0 {
			continue
		}

		if vs.lastFileNumber < uint64(number) {
			vs.lastFileNumber = uint64(number)
		}
	}
	return nil
}

func (vs *VersionSet) CurrentVersion() *Version {
	vs.versLock.RLock()
	defer vs.versLock.RUnlock()
	return vs.current
}

func (vs *VersionSet) RefVersion() *Version {
	vs.versLock.RLock()
	defer vs.versLock.RUnlock()
	vs.current.Ref()
	return vs.current
}

func (vs *VersionSet) setVersion(v *Version) {
	vs.versLock.Lock()
	defer vs.versLock.Unlock()
	vs.current = v
}

func (vs *VersionSet) LevelTableSize(level int) uint64 {
	return vs.opts.L0TableSize * (1 << level)
}

func (vs *VersionSet) SetSeekCompact(level int, meta *common.FileMetaData) {
	vs.versLock.Lock()
	defer vs.versLock.Unlock()
	vs.current.FileToCompact = meta
	vs.current.FileToCompactLevel = level
}

func (vs *VersionSet) RemoveUnrefVersion() []*Version {
	vs.versLock.Lock()
	defer vs.versLock.Unlock()
	vers := vs.dummyVersion.Next
	var unused []*Version
	for ; vers != nil; vers = vers.Next {
		if atomic.LoadInt64(&vers.ref) == 0 {
			unused = append(unused, vers)
			vers.prev.Next = vers.Next
		}
	}
	return unused
}
