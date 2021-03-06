package table

import (
	"Laputa/pkg/util"
	"encoding/binary"
	"errors"
)

// MagicNumber was picked by running
//    echo Jinvei/Laputa/leveldb | shasum | cut -c -16
const (
	MagicNumber uint64 = 0x11bdfe345779abc4

	BlockHandleEncodedLen = 10 + 10
	FooterEncodedLen      = 2*BlockHandleEncodedLen + 8 // 2*BlockHandle + MagicNumber

	BlockTrailerSize = 5 // 1-byte type + 32-bit crc

	CompressTypeNo     CompressType = 0x00
	CompressTypeSnappy CompressType = 0x01
)

type CompressType byte

var (
	ErrFooterEncodeLen      = errors.New("ErrFooterEncodeLen")
	ErrFooterValidMagic     = errors.New("ErrFooterValidMagic")
	ErrHandleDecodeBad      = errors.New("ErrHandleDecodeBad")
	ErrUnorderKey           = errors.New("ErrUnorderKey")
	ErrTableBlockCorruption = errors.New("ErrTableBlockCorruption")
)

type BlockHandle struct {
	Offset int64
	Size   int64
}

type Footer struct {
	MetaIndexHandle BlockHandle
	IndexHandle     BlockHandle
	// MagicNumber
}

func (h *BlockHandle) EncodeTo(out []byte) {
	util.Assert(20 <= len(out))
	binary.PutVarint(out[:10], h.Offset)
	binary.PutVarint(out[10:], h.Size)
}

func (h *BlockHandle) DecodeFrom(in []byte) error {
	util.Assert(20 <= len(in))
	n := 0
	h.Offset, n = binary.Varint(in[:10])
	if n <= 0 {
		return ErrHandleDecodeBad
	}
	h.Size, n = binary.Varint(in[10:])
	if n <= 0 {
		return ErrHandleDecodeBad
	}
	return nil
}

func (fo *Footer) EncodeTo(out []byte) {
	util.Assert(FooterEncodedLen <= len(out))

	fo.MetaIndexHandle.EncodeTo(out[:20])
	fo.IndexHandle.EncodeTo(out[20:40])
	binary.LittleEndian.PutUint64(out[40:48], MagicNumber)
}

func (fo *Footer) DecodeFrom(in []byte) error {
	if len(in) < FooterEncodedLen {
		return ErrFooterEncodeLen
	}

	if err := fo.MetaIndexHandle.DecodeFrom(in[:20]); err != nil {
		return err
	}

	if err := fo.IndexHandle.DecodeFrom(in[20:40]); err != nil {
		return err
	}

	magic := binary.LittleEndian.Uint64(in[40:48])
	if magic != MagicNumber {
		return ErrFooterValidMagic
	}

	return nil
}
