package kvdb_test

import (
	// "Laputa/pkg/kvdb"
	"Laputa/pkg/kvdb"
	"Laputa/pkg/kvdb/common"
	"fmt"
	"os"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"gotest.tools/assert"
)

func TestDB_Remove(t *testing.T) {
	opts := common.NewDefaultOptions()
	opts.DBDir = "./laputa_kvdb_test"
	defer func() {
		err := os.RemoveAll(opts.DBDir)
		if err != nil {
			fmt.Println(err)
		}
	}()
}
func TestDB_B(t *testing.T) {
	opts := common.NewDefaultOptions()
	opts.DBDir = "./laputa_kvdb_test"
	db := kvdb.NewDB(opts)
	err := db.Recover()
	assert.Assert(t, err == nil, err)
	cnt := 0
	for i := 0; i < 500000; i++ {
		if i%10000 == 0 {
			cnt++
			db.Close()
			db = kvdb.NewDB(opts)
			err := db.Recover()
			assert.Assert(t, err == nil, err)
		}
		err := db.Put([]byte(strconv.Itoa(i*100)), []byte(strconv.Itoa(i*200)))
		if err != nil {
			fmt.Println(err)
			time.Sleep(10 * time.Second)
		}
	}

	misscnt := 0
	for i := 0; i < 500000; i++ {
		_, ok := db.Get([]byte(strconv.Itoa(i * 100)))
		if !ok {
			misscnt++
		}
	}
	fmt.Println("misscnt", misscnt)
	db.Close()
	if misscnt != 0 {
		t.Fatal()
	}

	defer func() {
		err = os.RemoveAll(opts.DBDir)
		if err != nil {
			fmt.Println(err)
		}
	}()
}

var cnt = int64(0)

func BenchmarkKVDB(b *testing.B) {
	atomic.AddInt64(&cnt, 1)
	cnt1 := atomic.LoadInt64(&cnt)
	fmt.Println("cnt", cnt1, b.N)
	opts := common.NewDefaultOptions()
	//opts.MaxMemtableSize = 1024 * 1024 * 1024
	opts.DBDir = "./laputa_kvdb_test"
	db := kvdb.NewDB(opts)
	err := db.Recover()
	assert.Assert(b, err == nil, err)
	for i := 0; i < b.N; i++ {
		err := db.Put([]byte(strconv.Itoa(i*100)), []byte(strconv.Itoa(i*200)))
		if err != nil {
			fmt.Println(err)
		}
	}
	db.Close()

	fmt.Println("endcnt", cnt1)

	err = os.RemoveAll(opts.DBDir)
	if err != nil {
		fmt.Println(err)
	}
}

// func BenchmarkBadger(b *testing.B) {
// 	// Open the Badger database located in the /tmp/badger directory.
// 	// It will be created if it doesn't exist.
// 	opts := badger.DefaultOptions("./badger_db")
// 	//opts.SyncWrites = true
// 	db, err := badger.Open(opts)
// 	if err != nil {
// 		log.Fatal(err)
// 	}

// 	for i := 0; i < b.N; i++ {
// 		err = db.Update(func(txn *badger.Txn) error {
// 			err := txn.Set([]byte(strconv.Itoa(i*100)), []byte(strconv.Itoa(i*200)))
// 			return err
// 		})
// 	}
// 	defer db.Close()
// 	// Your code here…
// }

// func BenchmarkLevedblgo(b *testing.B) {
// 	o := &opt.WriteOptions{}
// 	o.Sync = false
// 	db, err := leveldb.OpenFile("./golangleveldb_test", nil)
// 	assert.Assert(b, err == nil, err)
// 	for i := 0; i < b.N; i++ {
// 		err := db.Put([]byte(strconv.Itoa(i*100)), []byte(strconv.Itoa(i*200)), o)
// 		assert.Assert(b, err == nil, err)
// 	}
// 	db.Close()

// }
