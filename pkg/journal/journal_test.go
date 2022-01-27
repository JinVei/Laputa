package journal

import (
	"fmt"
	"testing"
)

func TestLog(t *testing.T) {
	l, err := New("./raft.log")
	if err != nil {
		t.Fatal(err)
	}

	err = l.InitAndRecover(func(d []byte) {
		fmt.Println("data:", string(d))
	})
	if err != nil {
		t.Fatal(err)
	}

	err = l.Append([]byte("hello4"))
	if err != nil {
		t.Fatal(err)
	}

	// r, err := l.NewLogReader()
	// if err != nil {
	// 	t.Fatal(err)
	// }

	// err = r.ReplayAt(0, func(d []byte) {
	// 	fmt.Println("data:", string(d))
	// })
	// if err != nil {
	// 	t.Fatal(err)
	// }

}

func TestNewChunk(t *testing.T) {
	c := NewChunk(nil, 0x01)
	fmt.Println(c)
}
