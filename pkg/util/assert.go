package util

import "fmt"

func Assert(cond bool) {
	if !cond {
		panic("assert fail")
	}
}

func AssertWithMsg(cond bool, msg error) {
	if !cond {
		panic(fmt.Sprintln("Assert Fail", msg))
	}
}
