package common

/*
if k1 < k2,  ret < 0 (-1)
if k1 == k2, ret == 0
if k1 > k2,  0 < ret (+1)
*/

type Compare func(key1, key2 []byte) int
