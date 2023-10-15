package hash

import (
	"errors"
	"fmt"
	"io"
	"math"
	"sync"	

	pager "github.com/csci1270-fall-2023/dbms-projects-handout/pkg/pager"
	utils "github.com/csci1270-fall-2023/dbms-projects-handout/pkg/utils"
)

// HashTable definitions.
type HashTable struct {
	depth   int64
	buckets []int64 // Array of bucket page numbers
	pager   *pager.Pager
	rwlock  sync.RWMutex // Lock on the hash table index
}

// Returns a new HashTable.
func NewHashTable(pager *pager.Pager) (*HashTable, error) {
	depth := int64(2)
	buckets := make([]int64, powInt(2, depth))
	for i := range buckets {
		bucket, err := NewHashBucket(pager, depth)
		if err != nil {
			return nil, err
		}
		buckets[i] = bucket.page.GetPageNum()
		bucket.page.Put()
	}
	return &HashTable{depth: depth, buckets: buckets, pager: pager}, nil
}

// [CONCURRENCY] Grab a write lock on the hash table index
func (table *HashTable) WLock() {
	table.rwlock.Lock()
}

// [CONCURRENCY] Release a write lock on the hash table index
func (table *HashTable) WUnlock() {
	table.rwlock.Unlock()
}

// [CONCURRENCY] Grab a read lock on the hash table index
func (table *HashTable) RLock() {
	table.rwlock.RLock()
}

// [CONCURRENCY] Release a read lock on the hash table index
func (table *HashTable) RUnlock() {
	table.rwlock.RUnlock()
}

// Get depth.
func (table *HashTable) GetDepth() int64 {
	return table.depth
}

// Get bucket page numbers.
func (table *HashTable) GetBuckets() []int64 {
	return table.buckets
}

// Get pager.
func (table *HashTable) GetPager() *pager.Pager {
	return table.pager
}

// Finds the entry with the given key.
func (table *HashTable) Find(key int64) (utils.Entry, error) {
	// Hash the key.
	hash := Hasher(key, table.depth)
	if hash < 0 || int(hash) >= len(table.buckets) {
		return nil, errors.New("not found")
	}
	// Get and lock the corresponding bucket.
	bucket, err := table.GetBucket(hash)
	if err != nil {
		return nil, err
	}
	defer bucket.page.Put()
	// Find the entry.
	entry, found := bucket.Find(key)
	if !found {
		return nil, errors.New("not found")
	}
	return entry, nil
}

// ExtendTable increases the global depth of the table by 1.
func (table *HashTable) ExtendTable() {
	table.depth = table.depth + 1
	table.buckets = append(table.buckets, table.buckets...)
}

// Split the given bucket into two, extending the table if necessary.
func (table *HashTable) Split(bucket *HashBucket, hash int64) error {
	// If local depth less than global depth, just split bucket
	if bucket.GetDepth() < table.GetDepth() {
		new_bucket, bucket_error := NewHashBucket(table.GetPager(), bucket.depth + 1)
		if bucket_error != nil {
			return bucket_error
		}
		defer new_bucket.page.Put()
		table.buckets[hash] = new_bucket.page.GetPageNum()
		bucket.depth = bucket.GetDepth() + 1
		num_keys_in_overflowed_bucket := bucket.numKeys
		// Reassign entries in overflowed bucket
		for i := int64(0); i < num_keys_in_overflowed_bucket; i++ {
			current_key := bucket.getKeyAt(int64(i))
			current_value := bucket.getValueAt(int64(i))
			delete_error := bucket.Delete(current_key)
			if delete_error != nil {
				return delete_error
			}
			insert_error := table.Insert(current_key, current_value)
			if insert_error != nil {
				return insert_error
			}
		}
		return nil

	// 	If local depth is equal to global depth, double size of table AND split bucket
	} else if bucket.GetDepth() == table.GetDepth() {
		// Extend table, increase local depth of original and new buckets
		prev_global_depth := table.GetDepth()
		table.ExtendTable()
		new_bucket, bucket_error := NewHashBucket(table.GetPager(), bucket.depth + 1)
		if bucket_error != nil {
			return bucket_error
		}
		defer new_bucket.page.Put()
		bucket.depth = bucket.GetDepth() + 1

		// Grab only the x rightmost bits of the hash, where x is the old local depth
		bit_mask := (1 << uint(prev_global_depth)) - 1
		for i := len(table.buckets) / 2; i < len(table.buckets); i++ {
			// Maps the slot in the table that corresponds to the hash to the new bucket (using only x rightmost bits)
			if int64(i & bit_mask) == int64(hash & int64(bit_mask)) {
				table.buckets[i] = new_bucket.page.GetPageNum()
			// Anything that didn't cause a split will have 1 more pointer pointing to it now
				} else {
				table.buckets[i] = table.buckets[i & bit_mask]
			}
		}
		num_keys_in_overflowed_bucket := bucket.numKeys
		// Reassign entries in overflowed bucket
		for i := int64(0); i < num_keys_in_overflowed_bucket; i++ {
			current_key := bucket.getKeyAt(int64(i))
			current_value := bucket.getValueAt(int64(i))
			delete_error := bucket.Delete(current_key)
			if delete_error != nil {
				return delete_error
			}
			insert_error := table.Insert(current_key, current_value)
			if insert_error != nil {
				return insert_error
			}
		}
		return nil
	// Local depth is greater than global depth (which cannot be possible)
	} else {
		return fmt.Errorf("local depth is greater than global depth")
	}
}

// Inserts the given key-value pair, splits if necessary.
func (table *HashTable) Insert(key int64, value int64) error {
	// Hash the key and find which bucket the key should be inserted into
	hash := Hasher(key, table.GetDepth())
	bucket, err := table.GetBucket(hash)
	if err != nil {
		return err
	}
	split, insert_error := bucket.Insert(key, value)
	if insert_error != nil {
		return insert_error
	}
	//  Split if the bucket overflows
	if split {
		fmt.Println("split occurring!!!!!!!!!")
		fmt.Println(bucket.depth)
		fmt.Println(table.depth)
		fmt.Println(hash)
		split_error := table.Split(bucket, hash)
		if split_error != nil {
			return split_error
		}
	}
	return nil
}

// Update the given key-value pair.
func (table *HashTable) Update(key int64, value int64) error {
	hash := Hasher(key, table.depth)
	bucket, err := table.GetBucket(hash)
	if err != nil {
		return err
	}
	defer bucket.page.Put()
	return bucket.Update(key, value)
}

// Delete the given key-value pair, does not coalesce.
func (table *HashTable) Delete(key int64) error {
	hash := Hasher(key, table.depth)
	bucket, err := table.GetBucket(hash)
	if err != nil {
		return err
	}
	defer bucket.page.Put()
	return bucket.Delete(key)
}

// Select all entries in this table.
func (table *HashTable) Select() ([]utils.Entry, error) {
	// Create a new array of Entries
	entries := make([]utils.Entry, 0)
	// Iterate over every bucket in the table
	bucket_page_nums := table.GetBuckets()
	for i := 0; i < len(bucket_page_nums); i++ {
		current_bucket, get_bucket_error := table.GetBucketByPN(bucket_page_nums[i])
		if get_bucket_error != nil {
			return entries, get_bucket_error
		}
		// Iterate over every entry in the bucket
		for j := 0; j < int(current_bucket.numKeys); j++ {
			entry_to_add := current_bucket.getEntry(int64(j))
			entries = append(entries, entry_to_add)
		}
	}
	return entries, nil
}

// Print out each bucket.
func (table *HashTable) Print(w io.Writer) {
	table.RLock()
	defer table.RUnlock()
	io.WriteString(w, "====\n")
	io.WriteString(w, fmt.Sprintf("global depth: %d\n", table.depth))
	for i := range table.buckets {
		io.WriteString(w, fmt.Sprintf("====\nbucket %d\n", i))
		bucket, err := table.GetBucket(int64(i))
		if err != nil {
			continue
		}
		bucket.Print(w)
		bucket.RUnlock()
		bucket.page.Put()
	}
	io.WriteString(w, "====\n")
}

// Print out a specific bucket.
func (table *HashTable) PrintPN(pn int, w io.Writer) {
	table.RLock()
	defer table.RUnlock()
	if int64(pn) >= table.pager.GetNumPages() {
		fmt.Println("out of bounds")
		return
	}
	bucket, err := table.GetBucketByPN(int64(pn))
	if err != nil {
		return
	}
	bucket.Print(w)
	bucket.RUnlock()
	bucket.page.Put()
}

// x^y
func powInt(x, y int64) int64 {
	return int64(math.Pow(float64(x), float64(y)))
}
