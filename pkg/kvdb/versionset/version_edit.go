package versionset

import (
	"Laputa/pkg/kvdb/common"
	"encoding/binary"
	"io"
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

	compactPointer LevelInternalKeyPair
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
	n := binary.PutVarint(varintbuf, TagComparator)
	writer.Write(varintbuf[:n])
	common.EncodeLenPrefixBytes(writer, []byte(ve.comparator))

	n = binary.PutVarint(varintbuf, TagLogNumber)
	writer.Write(varintbuf[:n])
	n = binary.PutVarint(varintbuf, int64(ve.logNumber))
	writer.Write(varintbuf[:n])

	n = binary.PutVarint(varintbuf, TagNextFileNumber)
	writer.Write(varintbuf[:n])
	n = binary.PutVarint(varintbuf, int64(ve.nextFileNumber))
	writer.Write(varintbuf[:n])

	n = binary.PutVarint(varintbuf, TagLastSequence)
	writer.Write(varintbuf[:n])
	n = binary.PutVarint(varintbuf, int64(ve.lastSequence))
	writer.Write(varintbuf[:n])

	n = binary.PutVarint(varintbuf, TagCompactPointer)
	writer.Write(varintbuf[:n])
	n = binary.PutVarint(varintbuf, int64(ve.compactPointer.Level))
	writer.Write(varintbuf[:n])
	common.EncodeLenPrefixBytes(writer, ve.compactPointer.Key)

	if 0 < len(ve.deletedFiles) {
		n = binary.PutVarint(varintbuf, TagDeleteFile)
		writer.Write(varintbuf[:n])
	}
	for _, file := range ve.deletedFiles {
		n = binary.PutVarint(varintbuf, int64(file.Level))
		writer.Write(varintbuf[:n])
		n = binary.PutVarint(varintbuf, int64(file.Numner))
		writer.Write(varintbuf[:n])
	}

	if 0 < len(ve.newFiles) {
		n = binary.PutVarint(varintbuf, TagNewFile)
		writer.Write(varintbuf[:n])
	}
	for level, files := range ve.newFiles {
		for _, meta := range files {
			n = binary.PutVarint(varintbuf, int64(level))
			writer.Write(varintbuf[:n])
			common.EncodeFileMetaData(writer, *meta)
		}
	}
	return nil

}

func (ve *VersionEdit) Apply(edit VersionEdit) {
	ve.comparator = edit.comparator
	ve.logNumber = edit.logNumber
	ve.nextFileNumber = edit.nextFileNumber
	ve.lastSequence = edit.lastSequence
	ve.compactPointer = edit.compactPointer
	ve.deletedFiles = edit.deletedFiles

	for _, df := range edit.deletedFiles {
		for i, nf := range ve.newFiles[df.Level] {
			if df.Numner == nf.Number {
				ve.newFiles[df.Level] = append(ve.newFiles[df.Level][:i], ve.newFiles[df.Level][i+1:]...)
				break
			}
		}
	}
}

func (ve *VersionEdit) From(vers *Version) {
	ve.comparator = vers.Comparator
	ve.logNumber = vers.LogNumber
	ve.nextFileNumber = vers.NextFileNumber
	ve.lastSequence = vers.LastSequence
	//ve.compactPointer = vers.CompactPointer
	ve.newFiles = vers.MetaFiles // TODO
	ve.deletedFiles = nil
}

func (ve *VersionEdit) ApplyTo(vers *Version) {
	vers.Comparator = ve.comparator
	vers.LogNumber = ve.logNumber
	vers.NextFileNumber = ve.nextFileNumber
	vers.LastSequence = ve.lastSequence
	//vers.CompactPointer = ve.compactPointer
	vers.MetaFiles = ve.newFiles
}
