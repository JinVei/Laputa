package kvdb_test

import (
	"Laputa/pkg/kvdb"
	"Laputa/pkg/kvdb/common"
	"bytes"
	"testing"

	"gotest.tools/assert"
)

func TestDB(t *testing.T) {
	opts := common.NewDefaultOptions()
	db := kvdb.NewDB(opts)
	err := db.Recover()
	assert.Assert(t, err == nil, err)

	keyone := []byte("1")
	valueone := []byte("1_value")

	keyTwo := []byte("2")
	valueTwo := []byte("2_value")

	db.Put(keyone, valueone)

	db.Put(keyone, valueone)

	db.Put(keyTwo, valueTwo)

	val, exist := db.Get(keyone)
	assert.Assert(t, exist)
	assert.Assert(t, bytes.Compare(val, valueone) == 0, string(val)+":"+string(valueone))
}
