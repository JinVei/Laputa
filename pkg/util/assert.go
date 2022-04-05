package util

func Assert(cond bool) {
	if !cond {
		panic("assert fail")
	}
}
