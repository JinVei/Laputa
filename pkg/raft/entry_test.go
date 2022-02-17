package raft

import (
	raftPb "Laputa/api/raft/v1alpha1"
	"fmt"
	"testing"

	"google.golang.org/protobuf/proto"
)

func TestProtobufMarshalEntry(t *testing.T) {
	e := &raftPb.Entry{}
	e.Term = 1
	e.Index = 2
	e.Data = []byte("dd")
	b, err := proto.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(b)
}

func TestProtobufUnmarshalEntry(t *testing.T) {
	entryB := []byte{8, 1, 16, 2, 26, 2, 100, 100}
	e := &raftPb.Entry{}
	err := proto.Unmarshal(entryB, e)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(e)
}
