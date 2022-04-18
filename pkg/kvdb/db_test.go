package kvdb_test

import (
	// "Laputa/pkg/kvdb"
	"Laputa/pkg/kvdb"
	"Laputa/pkg/kvdb/common"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"gotest.tools/assert"
)

func TestDB_A(t *testing.T) {
	opts := common.NewDefaultOptions()
	opts.MaxMemtableSize = 100
	opts.DBDir = "./laputa_kvdb_test"
	db := kvdb.NewDB(opts)
	err := db.Recover()
	assert.Assert(t, err == nil, err)
	for i := 0; i < 100; i++ {
		err := db.Put([]byte(strconv.Itoa(i*100)), []byte(strconv.Itoa(i*200)))
		if err != nil {
			fmt.Println(err)
			time.Sleep(10 * time.Second)
		}
	}
	db.Close()

	err = os.RemoveAll(opts.DBDir)
	if err != nil {
		fmt.Println(err)
	}
}
