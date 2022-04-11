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
)

var (
	ErrUnknowVersionEditTag = errors.New("ErrUnknowVersionEditTag")
)

const (
	CurrentManifestName = "CURRENT"
	ManifestSufix       = ".manifest"
)

type Version struct {
	Comparator     string
	LogNumber      uint64
	NextFileNumber uint64
	LastSequence   uint64
	MetaFiles      [common.MaxLevels][]*common.FileMetaData

	Next *Version
	prev *Version

	compactionScore float64
	compactionLevel int

	fileToCompact      *common.FileMetaData
	fileToCompactLevel int

	vset *VersionSet
	ref  int
}

type Compaction struct {
	level        int
	inputs       [2][]*common.FileMetaData
	inputVersion *Version
}

func NewCompaction(opts *common.Options) *Compaction {
	c := &Compaction{}
	return c
}
func NewVersion(vset *VersionSet) *Version {
	v := new(Version)
	v.vset = vset
	return v
}

type VersionSet struct {
	manifestNumber uint64
	current        *Version
	dbname         string

	NextFileNumber uint64
	LastSequence   uint64
	LogNumber      uint64
	CompactPointer [common.MaxLevels]common.InternalKey

	lock         *sync.RWMutex
	dummyVersion Version

	manifest   *log.Writer //*os.File
	KeyCompare common.Compare
	opts       *common.Options
	//UserKeyCompare common.Compare

}

func NewVersionSet(dbname string, opts *common.Options) *VersionSet {
	vs := new(VersionSet)
	vs.dbname = dbname
	vs.lock = &sync.RWMutex{}
	vs.opts = opts
	vs.KeyCompare = opts.KeyComparator
	return vs
}

func (vs *VersionSet) PickCompaction() *Compaction {
	sizeCompacttion := 1 <= vs.current.compactionScore
	seekCompacttion := vs.current.fileToCompact != nil

	level := 0
	c := NewCompaction(vs.opts)
	if sizeCompacttion {
		level = vs.current.compactionLevel
		c.level = level
		metaFiles := vs.current.MetaFiles[level]
		for _, f := range metaFiles {
			if vs.CompactPointer[level] == nil ||
				vs.KeyCompare(f.Largest, vs.CompactPointer[level]) <= 0 {
				c.inputs[0] = append(c.inputs[0], f)
				break
			}
		}
		if len(c.inputs[0]) == 0 {
			c.inputs[0] = append(c.inputs[0], metaFiles[0])
		}

	} else if seekCompacttion {
		level = vs.current.compactionLevel
		c.level = level
		c.inputs[0] = append(c.inputs[0], vs.current.fileToCompact)
	} else {
		return nil
	}

	c.inputVersion = vs.current
	c.inputVersion.Ref()

	smallest, largest := vs.GetRange(c.inputs[0])
	smallest, largest, c.inputs[0] = vs.current.GetOverlappingInputs(level, smallest, largest, c.inputs[0])

	smallest, largest, c.inputs[1] = vs.current.GetOverlappingInputs(level+1, smallest, largest, c.inputs[1])

	vs.CompactPointer[level] = largest
	return c
}

func (vs *VersionSet) LogAndApply(edit *VersionEdit) {
	ve := VersionEdit{}
	ve.From(vs.current)
	ve.Apply(*edit)

	newVersion := NewVersion(vs)
	ve.ApplyTo(newVersion)
	record := bytes.NewBuffer(make([]byte, 0))
	ve.EncodeTo(record)
	vs.manifest.AddRecord(record.Bytes())

	vs.lock.Lock()
	defer vs.lock.Unlock()

	vs.current.prev = &vs.dummyVersion
	vs.current.Next = vs.dummyVersion.Next
	vs.dummyVersion.Next = vs.current
	vs.current.Unref()
	vs.current = newVersion
}

func (vs *VersionSet) Recover() error {
	vs.lock.Lock()
	defer vs.lock.Unlock()

	if err := os.MkdirAll(vs.dbname, 0766); err != nil && err != os.ErrExist {
		return err
	}

	current := filepath.Join(vs.dbname, CurrentManifestName)
	fcurrent, err := os.OpenFile(current, os.O_CREATE|os.O_RDWR, 0766)
	if err != nil {
		return err
	}
	defer fcurrent.Close()

	manifestNum, err := ioutil.ReadAll(fcurrent)
	if err != nil && err != io.EOF {
		return err
	}
	var ve *VersionEdit
	oldmanifestPath := filepath.Join(vs.dbname, string(manifestNum)+ManifestSufix)
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
		for {
			logReader := log.NewReader(fmanifest)
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

			if ve == nil {
				ve = new(VersionEdit)
			}
			ve.Apply(v)
		}
	}
END_READ_MANIFEST:
	version := NewVersion(vs)
	if ve != nil {
		ve.ApplyTo(version)
	}
	version.Ref()
	vs.current = version

	mewManifestNum := strconv.Itoa(int(vs.current.NextFileNumber))
	newmanifest := filepath.Join(vs.dbname, mewManifestNum+ManifestSufix)
	vs.current.NextFileNumber++
	if vs.current.LogNumber == 0 {
		vs.current.LogNumber = vs.current.NextFileNumber
		vs.current.NextFileNumber++
	}

	fmanifest, err := os.OpenFile(newmanifest, os.O_CREATE|os.O_TRUNC|os.O_WRONLY|os.O_APPEND, 0766)
	if err != nil {
		return err
	}
	vs.manifest = log.NewWriter(fmanifest)

	edit := VersionEdit{}
	edit.From(vs.current)
	record := bytes.NewBuffer(make([]byte, 0))
	edit.EncodeTo(record)
	vs.manifest.AddRecord(record.Bytes())

	vs.NextFileNumber = vs.current.NextFileNumber
	vs.LastSequence = vs.current.LastSequence
	vs.LogNumber = vs.current.LogNumber

	if _, err := fcurrent.WriteAt([]byte(mewManifestNum), 0); err != nil {
		return err
	}
	os.Remove(oldmanifestPath)
	fcurrent.Sync()

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

func (v *Version) Ref() { v.ref++ }
func (v *Version) Unref() {
	v.ref--
	if v.ref == 0 {
		v.prev.Next = v.Next
		v.prev = nil
		v.Next = nil
	}
	//TODO: unref filemeta
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
