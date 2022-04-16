package versionset

import (
	"Laputa/pkg/kvdb/common"
	"encoding/binary"
	"io"
	"sort"
)

type LevelFileMetaDataPair struct {
	Level int
	Meta  common.FileMetaData
}

type LevelInternalKeyPair struct {
	Level int
	Key   common.InternalKey
}

type LevelNumberPair struct {
	Level  int
	Numner uint64
}

const (
	TagComparator = iota + 1
	TagLogNumber
	TagNextFileNumber
	TagLastSequence
	TagCompactPointer
	TagDeleteFile
	TagNewFile
)

type VersionEdit struct {
	comparator     string
	logNumber      uint64
	nextFileNumber uint64
	lastSequence   uint64

	compactPointer *LevelInternalKeyPair
	deletedFiles   []LevelNumberPair
	newFiles       [common.MaxLevels][]*common.FileMetaData
}

func (ve *VersionEdit) DecodeFrom(src []byte) error {
	offset := 0
	for offset < len(src) {
		tag, n := binary.Varint(src[offset:])
		offset += n
		switch tag {
		case TagComparator:
			name, n := common.DecodeLenPrefixBytes(src[offset:])
			ve.comparator = string(name)
			offset += n
		case TagLogNumber:
			logNumber, n := binary.Varint(src[offset:])
			offset += n
			ve.logNumber = uint64(logNumber)
		case TagNextFileNumber:
			nextFileNumber, n := binary.Varint(src[offset:])
			offset += n
			ve.nextFileNumber = uint64(nextFileNumber)
		case TagLastSequence:
			sequence, n := binary.Varint(src[offset:])
			offset += n
			ve.lastSequence = uint64(sequence)
		case TagCompactPointer:
			level, n := binary.Varint(src[offset:])
			offset += n
			compactPointer, n := common.DecodeLenPrefixBytes(src[offset:])
			offset += n
			if ve.compactPointer == nil {
				ve.compactPointer = new(LevelInternalKeyPair)
			}
			ve.compactPointer.Level = int(level)
			ve.compactPointer.Key = common.InternalKey(compactPointer)
		case TagDeleteFile:
			level, n := binary.Varint(src[offset:])
			offset += n

			fileNumber, n := binary.Varint(src[offset:])
			offset += n

			pair := LevelNumberPair{
				Level:  int(level),
				Numner: uint64(fileNumber),
			}

			ve.deletedFiles = append(ve.deletedFiles, pair)
		case TagNewFile:
			level, n := binary.Varint(src[offset:])
			offset += n

			meta, n := common.DecodeFileMetaData(src[offset:])
			offset += n

			ve.newFiles[level] = append(ve.newFiles[level], &meta)
		default:
			return ErrUnknowVersionEditTag
		}
	}
	return nil
}

func (ve *VersionEdit) EncodeTo(writer io.Writer) error {
	varintbuf := make([]byte, 10)
	n := 0
	if ve.comparator != "" {
		n = binary.PutVarint(varintbuf, TagComparator)
		writer.Write(varintbuf[:n])
		common.EncodeLenPrefixBytes(writer, []byte(ve.comparator))
	}

	if ve.logNumber != 0 {
		n = binary.PutVarint(varintbuf, TagLogNumber)
		writer.Write(varintbuf[:n])
		n = binary.PutVarint(varintbuf, int64(ve.logNumber))
		writer.Write(varintbuf[:n])
	}

	if ve.nextFileNumber != 0 {
		n = binary.PutVarint(varintbuf, TagNextFileNumber)
		writer.Write(varintbuf[:n])
		n = binary.PutVarint(varintbuf, int64(ve.nextFileNumber))
		writer.Write(varintbuf[:n])
	}

	if ve.lastSequence != 0 {
		n = binary.PutVarint(varintbuf, TagLastSequence)
		writer.Write(varintbuf[:n])
		n = binary.PutVarint(varintbuf, int64(ve.lastSequence))
		writer.Write(varintbuf[:n])
	}

	if ve.compactPointer != nil {
		n = binary.PutVarint(varintbuf, TagCompactPointer)
		writer.Write(varintbuf[:n])
		n = binary.PutVarint(varintbuf, int64(ve.compactPointer.Level))
		writer.Write(varintbuf[:n])
		common.EncodeLenPrefixBytes(writer, ve.compactPointer.Key)
	}

	for _, file := range ve.deletedFiles {
		n = binary.PutVarint(varintbuf, TagDeleteFile)
		writer.Write(varintbuf[:n])

		n = binary.PutVarint(varintbuf, int64(file.Level))
		writer.Write(varintbuf[:n])
		n = binary.PutVarint(varintbuf, int64(file.Numner))
		writer.Write(varintbuf[:n])
	}

	for level, files := range ve.newFiles {
		for _, meta := range files {
			n = binary.PutVarint(varintbuf, TagNewFile)
			writer.Write(varintbuf[:n])
			n = binary.PutVarint(varintbuf, int64(level))
			writer.Write(varintbuf[:n])
			common.EncodeFileMetaData(writer, *meta)
		}
	}
	return nil

}

func (ve *VersionEdit) Apply(edit VersionEdit) {
	if edit.comparator != "" {
		ve.comparator = edit.comparator
	}
	if edit.logNumber != 0 {
		ve.logNumber = edit.logNumber
	}
	if edit.nextFileNumber != 0 {
		ve.nextFileNumber = edit.nextFileNumber
	}
	if edit.lastSequence != 0 {
		ve.lastSequence = edit.lastSequence
	}
	if edit.compactPointer != nil {
		ve.compactPointer = edit.compactPointer
	}

	for level, mfile := range edit.newFiles {
		for _, file := range mfile {
			ve.newFiles[level] = append(ve.newFiles[level], file)
		}
	}

	for _, df := range edit.deletedFiles {
		found := false
		for i, f := range ve.newFiles[df.Level] {
			if f.Number == df.Numner {
				ve.newFiles[df.Level] = append(ve.newFiles[df.Level][:i], ve.newFiles[df.Level][i+1:]...)
				found = true
				break
			}
		}
		if !found {
			ve.deletedFiles = append(ve.deletedFiles, df)
		}
	}
}

func (ve *VersionEdit) SetLogNumber(number uint64) {
	ve.logNumber = number
}

func (ve *VersionEdit) SetNextFileNumber(number uint64) {
	ve.nextFileNumber = number
}

func (ve *VersionEdit) SetLastSequence(sequence uint64) {
	ve.nextFileNumber = sequence
}

func (ve *VersionEdit) SetComparator(name string) {
	ve.comparator = name
}

func (ve *VersionEdit) AddFile(level int, file *common.FileMetaData) {
	ve.newFiles[level] = append(ve.newFiles[level], file)
}

func (ve *VersionEdit) DelFile(level int, number uint64) {
	ve.deletedFiles = append(ve.deletedFiles, LevelNumberPair{
		Level:  level,
		Numner: number,
	})
}

func NewVersion(vset *VersionSet, oldv *Version, edit *VersionEdit) *Version {
	v := new(Version)
	v.vset = vset

	if oldv != nil {
		v.Comparator = oldv.Comparator
		v.LogNumber = oldv.LogNumber
		v.NextFileNumber = oldv.NextFileNumber
		v.LastSequence = oldv.LastSequence

		if oldv.compactPointer != nil {
			v.compactPointer = &LevelInternalKeyPair{
				Level: oldv.compactPointer.Level,
				Key:   oldv.compactPointer.Key,
			}
		}

		for level, metaf := range oldv.MetaFiles {
			v.MetaFiles[level] = make([]*common.FileMetaData, len(metaf))
			copy(v.MetaFiles[level], metaf)
		}
	}

	if edit != nil {
		if v.LogNumber < edit.logNumber {
			v.LogNumber = edit.logNumber
		}
		if v.NextFileNumber < edit.nextFileNumber {
			v.NextFileNumber = edit.nextFileNumber
		}
		if v.LastSequence < edit.lastSequence {
			v.LastSequence = edit.lastSequence
		}
		if edit.compactPointer != nil {
			v.compactPointer = edit.compactPointer
		}

		for level, metas := range edit.newFiles {
			v.MetaFiles[level] = append(v.MetaFiles[level], metas...)

			// descend by number
			sort.Sort(sortMetaFile(v.MetaFiles[level]))
		}

		for _, delf := range edit.deletedFiles {
			for i, f := range v.MetaFiles[delf.Level] {
				if f.Number == delf.Numner {
					v.MetaFiles[delf.Level] = append(v.MetaFiles[delf.Level][:i], v.MetaFiles[delf.Level][i+1:]...)
					break
				}
			}
		}
	}
	return v
}

type sortMetaFile []*common.FileMetaData

func (s sortMetaFile) Len() int {
	return len(s)
}

func (s sortMetaFile) Less(i, j int) bool {
	// descend by file number
	return s[j].Number < s[i].Number
}

func (s sortMetaFile) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}
