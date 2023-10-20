package query

import (
	bitset "github.com/bits-and-blooms/bitset"
	"github.com/csci1270-fall-2023/dbms-projects-handout/pkg/hash"
	// hash "github.com/csci1270-fall-2023/dbms-projects-handout/pkg/hash"
)

type BloomFilter struct {
	size int64
	bits *bitset.BitSet
}

// CreateFilter initializes a BloomFilter with the given size.
func CreateFilter(size int64) *BloomFilter {
	bits := bitset.New(uint(size))
	return &BloomFilter{size: DEFAULT_FILTER_SIZE, bits: bits}
}

// Insert adds an element into the bloom filter.
func (filter *BloomFilter) Insert(key int64) {
	xxxHashResult := hash.XxHasher(key, filter.size)
	murmurhashResult := hash.MurmurHasher(key, filter.size)
	filter.bits.Set(xxxHashResult)
	filter.bits.Set(murmurhashResult)
}

// Contains checks if the given key can be found in the bloom filter/
func (filter *BloomFilter) Contains(key int64) bool {
		xxxHashResult := hash.XxHasher(key, filter.size)
		murmurHashResult := hash.MurmurHasher(key, filter.size)
		if filter.bits.Test(xxxHashResult) && filter.bits.Test(murmurHashResult) {
			return true
		}
		return false
}