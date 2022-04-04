package random

const (
	M = uint32(2147483647) // 2^31-1
	A = uint32(16807)      // bits 14, 8, 7, 5, 2, 1, 0
)

type Random struct {
	seed uint32
}

func New(s uint32) *Random {
	r := &Random{}
	if s == 0 || s == 2147483647 {
		s = 1
	}
	r.seed = s & 0x7fffffff

	return r
}

func (r *Random) Next() uint32 {
	product := uint64(r.seed) * uint64(A)
	r.seed = uint32(product>>31) + (uint32(product) & M)

	// The first reduction may overflow by 1 bit
	if r.seed > M {
		r.seed -= M
	}

	return r.seed
}

// Returns a uniformly distributed value in the range [0..n-1]
// REQUIRES: n > 0
func (r *Random) Uniform(n int) uint32 {
	return r.Next() % uint32(n)
}

// Randomly returns true ~"1/n" of the time, and false otherwise.
// REQUIRES: n > 0
func (r *Random) OneIn(n int) bool {
	return (r.Next() % uint32(n)) == 0
}

// Skewed: pick "base" uniformly from range [0,max_log] and then
// return "base" random bits.  The effect is to pick a number in the
// range [0,2^max_log-1] with exponential bias towards smaller numbers.
func (r *Random) Skewed(max_log int) uint32 {
	return r.Uniform(1 << r.Uniform(max_log+1))
}
