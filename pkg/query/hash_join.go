package query

import (
	"context"
	"fmt"
	"os"

	db "github.com/csci1270-fall-2023/dbms-projects-handout/pkg/db"
	hash "github.com/csci1270-fall-2023/dbms-projects-handout/pkg/hash"
	utils "github.com/csci1270-fall-2023/dbms-projects-handout/pkg/utils"

	errgroup "golang.org/x/sync/errgroup"
)

var DEFAULT_FILTER_SIZE int64 = 1024

// Entry pair struct - output of a join.
type EntryPair struct {
	l utils.Entry
	r utils.Entry
}

// Int pair struct - to keep track of seen bucket pairs.
type pair struct {
	l int64
	r int64
}

// buildHashIndex constructs a temporary hash table for all the entries in the given sourceTable.
func buildHashIndex(
	sourceTable db.Index,
	useKey bool, // hashing based on the key or the value, determines whether you want to swap key and value
) (tempIndex *hash.HashIndex, dbName string, err error) {
	// Get a temporary db file.
	dbName, err = db.GetTempDB()
	if err != nil {
		return nil, dbName, err
	}
	// Init the temporary hash table.
	tempIndex, err = hash.OpenTable(dbName)
	if err != nil {
		return nil, dbName, err
	}
	// Build the hash index by initializing a cursor (you can go without a cursor and just grab all entries of the table,
	// but you would have to assume all entries fit in main memory, which works in this case)
	cursor, cursor_error := sourceTable.TableStart()
	if cursor_error != nil {
		return nil, dbName, cursor_error
	}
	// Loop over all entries using cursor
	for {
		if !cursor.IsEnd() {
			// Get entry
			current_entry, get_entry_error := cursor.GetEntry()
			if get_entry_error != nil {
				return nil, dbName, get_entry_error
			}
			// Extract key and value from entry
			current_key := current_entry.GetKey()
			current_value := current_entry.GetValue()
			fmt.Print(current_key)
			fmt.Print(current_value)
			if useKey {
				// Use table key as actual hash table key
				tempIndex.Insert(current_key, current_value)		
			} else {
				// Use table value as actual hash table key
				tempIndex.Insert(current_value, current_key)
			}	
		}
		if cursor.StepForward() {
			break
		}
	}
	return tempIndex, dbName, nil
}

// sendResult attempts to send a single join result to the resultsChan channel as long as the errgroup hasn't been cancelled.
func sendResult(
	ctx context.Context,
	resultsChan chan EntryPair,
	result EntryPair,
) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case resultsChan <- result:
		return nil
	}
}

// See which entries in rBucket have a match in lBucket.
func probeBuckets(
	ctx context.Context,
	resultsChan chan EntryPair,
	// These above 2 arguments are just passed directly into sendResult
	lBucket *hash.HashBucket,
	rBucket *hash.HashBucket,
	joinOnLeftKey bool,
	joinOnRightKey bool,
) error {
	defer lBucket.GetPage().Put()
	defer rBucket.GetPage().Put()
	// Scan inner relation and use hash function to see
	right_bucket_entries, select_err := rBucket.Select()
	if select_err != nil {
		return select_err
	}
	left_bucket_entries, select_err := lBucket.Select()
	if select_err != nil {
		return select_err
	}
	// Create and populate bloom filter
	bloom_filter := CreateFilter(DEFAULT_FILTER_SIZE)
	for i := 0; i < len(right_bucket_entries); i++ {
		current_key := right_bucket_entries[i].GetKey()
		bloom_filter.Insert(current_key)
	}

	for j := 0; j < len(left_bucket_entries); j++ {
		if !bloom_filter.Contains(left_bucket_entries[j].GetKey()) {
			continue

		} else {
			for i := 0; i < len(right_bucket_entries); i++ {

				current_left_key := left_bucket_entries[j].GetKey()
				current_right_key := right_bucket_entries[i].GetKey()
				
				if (current_left_key == current_right_key) {
						current_left_value := left_bucket_entries[j].GetValue()
						current_right_value := right_bucket_entries[i].GetValue()
						// Set the left and right entries upon finding a match
						left_entry := hash.HashEntry{}
						right_entry := hash.HashEntry{}

						// Swap both left and right entries
						if !joinOnLeftKey && !joinOnRightKey {
							right_entry.SetKey(current_right_value)
							right_entry.SetValue(current_right_key)
							left_entry.SetKey(current_left_value)
							left_entry.SetValue(current_left_key)
						
						// Swap only the left entry
						} else if !joinOnLeftKey && joinOnRightKey {
							left_entry.SetKey(current_left_value)
							left_entry.SetValue(current_left_key)
							right_entry.SetKey(current_right_key)
							right_entry.SetValue(current_right_value)
						
						// Swap only the right entry
						} else if joinOnLeftKey && !joinOnRightKey {
							right_entry.SetKey(current_right_value)
							right_entry.SetValue(current_right_key)
							left_entry.SetKey(current_left_key)
							left_entry.SetValue(current_left_value)
						
						// Swap neither entry
						} else {
							right_entry.SetKey(current_right_key)
							right_entry.SetValue(current_right_value)
							left_entry.SetKey(current_left_key)
							left_entry.SetValue(current_left_value)
						}
						sendResult(ctx, resultsChan, EntryPair{l: left_entry, r: right_entry})
			}
		}
		}
	}
	return nil
}

// Join leftTable on rightTable using Grace Hash Join.
func Join(
	ctx context.Context,
	leftTable db.Index,
	rightTable db.Index,
	joinOnLeftKey bool,
	joinOnRightKey bool,
) (chan EntryPair, context.Context, *errgroup.Group, func(), error) {
	leftHashIndex, leftDbName, err := buildHashIndex(leftTable, joinOnLeftKey)
	// join on left key tells us if the left bucket's useKey is true or not
	if err != nil {
		return nil, nil, nil, nil, err
	}
	rightHashIndex, rightDbName, err := buildHashIndex(rightTable, joinOnRightKey)
	// join on right key tells us if the right bucket's useKey is true or not
	if err != nil {
		os.Remove(leftDbName)
		os.Remove(leftDbName + ".meta")
		return nil, nil, nil, nil, err
	}
	cleanupCallback := func() {
		os.Remove(leftDbName)
		os.Remove(leftDbName + ".meta")
		os.Remove(rightDbName)
		os.Remove(rightDbName + ".meta")
	}
	// Make both hash indices the same global size.
	leftHashTable := leftHashIndex.GetTable()
	rightHashTable := rightHashIndex.GetTable()
	for leftHashTable.GetDepth() != rightHashTable.GetDepth() {
		if leftHashTable.GetDepth() < rightHashTable.GetDepth() {
			// Split the left table
			leftHashTable.ExtendTable()
		} else {
			// Split the right table
			rightHashTable.ExtendTable()
		}
	}
	// Probe phase: match buckets to buckets and emit entries that match.
	group, ctx := errgroup.WithContext(ctx)
	resultsChan := make(chan EntryPair, 1024)
	// Iterate through hash buckets, keeping track of pairs we've seen before.
	leftBuckets := leftHashTable.GetBuckets()
	rightBuckets := rightHashTable.GetBuckets()
	seenList := make(map[pair]bool)
	for i, lBucketPN := range leftBuckets {
		rBucketPN := rightBuckets[i]
		bucketPair := pair{l: lBucketPN, r: rBucketPN}
		if _, seen := seenList[bucketPair]; seen {
			continue
		}
		seenList[bucketPair] = true

		lBucket, err := leftHashTable.GetBucketByPN(lBucketPN)
		if err != nil {
			return nil, nil, nil, cleanupCallback, err
		}
		rBucket, err := rightHashTable.GetBucketByPN(rBucketPN)
		if err != nil {
			lBucket.GetPage().Put()
			return nil, nil, nil, cleanupCallback, err
		}
		group.Go(func() error {
			return probeBuckets(ctx, resultsChan, lBucket, rBucket, joinOnLeftKey, joinOnRightKey)
		})
	}
	return resultsChan, ctx, group, cleanupCallback, nil
}
