package kvdb

import (
	// "Laputa/pkg/kvdb"
	"Laputa/pkg/kvdb/common"
	"Laputa/pkg/kvdb/memtable"
	"Laputa/pkg/kvdb/table"
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"gotest.tools/assert"
)

func TestDB(t *testing.T) {
	opts := common.NewDefaultOptions()
	db := NewDB(opts)
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
	time.Sleep(5 * time.Minute)
}

func TestDBTable(t *testing.T) {
	opts := common.NewDefaultOptions()
	f, err := os.OpenFile(filepath.Join(opts.DBDir, common.GetTableName(4)), os.O_RDONLY, 0766)
	if err != nil {
		panic(err)
	}
	tb := table.New(opts)
	if err := tb.Open(f); err != nil {
		f.Close()
		panic(err)
	}
}

func TestMinorCompact(t *testing.T) {
	opts := common.NewDefaultOptions()
	mtable := memtable.New(opts.KeyComparator)
	db := NewDB(opts)
	db.Recover()
	for i := 0; i < 100; i++ {
		mtable.Put(uint64(i), common.KTypeValue, []byte(strconv.Itoa(i)), []byte(strconv.Itoa(i))) // seq = 3
	}
	err := db.doMinorCompact(mtable, 199, 198)
	if err != nil {
		fmt.Println(err)
	}

}

func TestDB_A(t *testing.T) {
	opts := common.NewDefaultOptions()
	opts.MaxMemtableSize = 100
	db := NewDB(opts)
	err := db.Recover()
	assert.Assert(t, err == nil, err)
	for i := 0; i < 100; i++ {
		err := db.Put([]byte(strconv.Itoa(i*100)), []byte(strconv.Itoa(i*200)))
		if err != nil {
			fmt.Println(err)
			time.Sleep(10 * time.Second)
		}
	}

}
