package util

func Assert(cond bool) {
	if !cond {
		panic("asser fail")
	}
}
