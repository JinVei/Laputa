package log

import "errors"

type RecordType uint8

// Log format:
//  --- Block ----  <- 32K aligned
//  --- Block ----
//  ---  ...  ----
//  --- Block ----

// Block format:
//  ---- Block ----
//    -- record --
//    -- record --
//    --  ...   --
//    -- record --
//  ---- Block ----   <- 32K aligned

// Record format:
//   ---  header    ---  (crc:4|length:2|type:1) 7 bytes
//   --- data-slice ---

const (
	RecordTypeZere = RecordType(0)

	RecordTypeFull   = RecordType(1)
	RecordTypeFirst  = RecordType(2)
	RecordTypeMiddle = RecordType(3)
	RecordTypeLast   = RecordType(4)

	BlockSize = 32768
	// Header consists of checksum (4 bytes), length (2 bytes), record type (1 byte).
	HeaderSize = 4 + 2 + 1
)

var (
	ErrCorruption = errors.New("Corruption Error")
)
