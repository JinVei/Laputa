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

func TestAppend(t *testing.T) {
	l, err := New("./raft.log")
	if err != nil {
		t.Fatal(err)
	}

	if err := l.InitAndRecover(nil); err != nil {
		t.Fatal(err)
	}

	// s1 := rand.NewSource(time.Now().UnixNano())
	// r1 := rand.New(s1)
	// for i := 0; i < 50; i++ {
	// 	r := r1.Intn(100)
	// 	size := r * 1024
	// 	d := make([]byte, size)
	// 	if err := l.Append(d); err != nil {
	// 		t.Fatal(err)
	// 	}
	// }
	d := make([]byte, 100*1024)
	if err := l.Append(d); err != nil {
		t.Fatal(err)
	}

}

func TestReadRecord(t *testing.T) {
	l, err := New("./raft.log")
	if err != nil {
		t.Fatal(err)
	}
	i := 0
	r, err := l.NewJournalReader()
	if err != nil {
		t.Fatal(err)
	}
	err = r.ReplayAt(0, func(d []byte) {
		fmt.Println(i, len(d)/1024)
		i++
	})
	if err != nil {
		t.Fatal(err)
	}
}
