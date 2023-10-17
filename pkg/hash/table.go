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
	new_bucket, err := NewHashBucket(table.pager, bucket.depth + 1)
	if err != nil {
		return err
	}
	defer new_bucket.page.Put()
	entries, entry_err := bucket.Select()
	if entry_err != nil {
		return entry_err
	}
	for _, entry := range entries {
		deletion_error := bucket.Delete(entry.GetKey())
		if deletion_error != nil {
			return deletion_error
		}
	}
	bucket.updateDepth(bucket.depth + 1)
	if bucket.depth <= table.GetDepth() {
		// Reassign pointers
		if hash + powInt(2, (table.GetDepth() - 1)) <= powInt(2, table.GetDepth()) - 1 {
			table.buckets[hash + powInt(2, (table.GetDepth() - 1))] = new_bucket.page.GetPageNum()
		} else {
			table.buckets[hash - powInt(2, (table.GetDepth() - 1))] = new_bucket.page.GetPageNum()
		}

		for _, entry := range entries {
			insertion_error := table.Insert(entry.GetKey(), entry.GetValue())
			if insertion_error != nil {
				return insertion_error
			}
		}
		return nil

	} else {
		prev_table_depth := table.GetDepth() - 1
		table.ExtendTable()
		for i := powInt(2, (table.GetDepth())) / 2; i < powInt(2, (table.GetDepth())); i++ {
			if hash != i - powInt(2, prev_table_depth) {
				table.buckets[i] = table.buckets[i - powInt(2, prev_table_depth)]
			} else {
				table.buckets[i] = new_bucket.page.GetPageNum()
			}
		}
		for _, entry := range entries {
			insertion_err := table.Insert(entry.GetKey(), entry.GetValue())
			if insertion_err != nil {
				return insertion_err
			}
		}
		return nil
	}
}

// Inserts the given key-value pair, splits if necessary.
func (table *HashTable) Insert(key int64, value int64) error {
	hash := Hasher(key, table.GetDepth())
	bucket, get_bucket_function_error := table.GetBucket(hash)
	
	if get_bucket_function_error != nil {
		return get_bucket_function_error
	}

	defer bucket.page.Put()
	bucket_split, bucket_insertion_error := bucket.Insert(key, value)

	if bucket_insertion_error != nil {
		return bucket_insertion_error
	}

	if bucket_split {
		split_err := table.Split(bucket, hash)
		if split_err != nil {
			return split_err
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
	panic("function not yet implemented")
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
