package versionset

import (
	"Laputa/pkg/kvdb/common"
	"bytes"
	"fmt"
	"testing"

	"gotest.tools/assert"
)

func TestVersionSet(t *testing.T) {
	opts := common.NewDefaultOptions()
	vset := NewVersionSet("./db", opts)
	err := vset.Recover()
	assert.Assert(t, err == nil, err)
}

func TestPickcompaction(t *testing.T) {

	opts := common.NewDefaultOptions()
	vset := NewVersionSet("./db", opts)
	err := vset.Recover()
	assert.Assert(t, err == nil, err)

	buf := bytes.NewBuffer(make([]byte, 0))

	meta1 := common.FileMetaData{}
	common.EncodeInternalKey(buf, []byte("aab"), 1, common.KTypeValue)
	meta1.Largest = buf.Next(10000)
	buf.Reset()
	common.EncodeInternalKey(buf, []byte("aaa"), 2, common.KTypeValue)
	meta1.Smallest = buf.Next(10000)
	meta1.Number = 12
	meta1.FileSize = 100

	meta2 := common.FileMetaData{}
	common.EncodeInternalKey(buf, []byte("bbbc"), 3, common.KTypeValue)
	meta2.Largest = buf.Next(10000)
	buf.Reset()
	common.EncodeInternalKey(buf, []byte("bbbb"), 4, common.KTypeValue)
	meta2.Smallest = buf.Next(10000)
	meta2.Number = 13
	meta2.FileSize = 100

	meta3 := common.FileMetaData{}
	common.EncodeInternalKey(buf, []byte("bbaaa"), 5, common.KTypeValue)
	meta3.Largest = buf.Next(10000)
	buf.Reset()
	common.EncodeInternalKey(buf, []byte("bbdddd"), 6, common.KTypeValue)
	meta3.Smallest = buf.Next(10000)
	meta3.Number = 13
	meta3.FileSize = 100

	vset.current.MetaFiles[0] = append(vset.current.MetaFiles[0], &meta1)
	vset.current.MetaFiles[0] = append(vset.current.MetaFiles[0], &meta2)
	vset.current.MetaFiles[1] = append(vset.current.MetaFiles[1], &meta3)
	vset.current.compactionScore = 1
	vset.current.compactionLevel = 0

	c := vset.PickCompaction()
	fmt.Println(c)
}
