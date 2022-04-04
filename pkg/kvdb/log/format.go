package log

type RecordType uint8

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
