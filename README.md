# Database

## Paging
The paging feature is a critical component of the database system that manages how data is stored, retrieved, and manipulated in memory-efficient blocks called **_pages_**. Each page acts as a container for data, facilitating efficient storage and minimizing input/output (I/O) operations during database queries. 

Pages are typically 4096 bytes in size, aligning with the typical page size in modern operating systems. Pages are used to store records or tuples, and the pager ensures that these pages are read from disk and written back to disk in a way that minimizes overhead. The key tasks of the pager include:

1. **Fetching Pages:** When a page is requested, the pager checks if it already exists in memory. If not, it fetches the page from disk, loads it into memory, and updates its internal page table to reflect the new memory location.
2. **Evicting Pages:** To make room for new pages in memory, the pager must evict unused or less recently used pages. This is done using a least recently used (LRU) eviction policy, implemented by tracking pages in the unpinned list.
3. **Flushing Pages:** Any pages that have been modified in memory (dirty pages) must be written back to disk before they are evicted, ensuring data consistency.

### Pager Design and Implementation
The pager's design revolves around managing a buffer cache that stores pages in memory. The buffer is implemented as a set of frames, where each frame holds a page. The pager uses a page table to map page IDs to their corresponding frames in the buffer. The main components of the pager are:

- **Free List:** A queue of pre-allocated pages that are available for use.
- **Unpinned List:** A list of pages in memory that are not currently in use and are candidates for eviction when new pages need to be loaded.
- **Pinned List:** A list of pages that are currently in use, represented by pages with a non-zero pin count. These pages cannot be evicted until their pin count reaches zero.

The pager also ensures **mutual exclusion** through the use of pinning. Pinning a page prevents it from being evicted from memory while it is in use. The pager implements this through a pin count, where pages with a non-zero pin count are marked as "in use."

### Core Functions of the Pager
The pager exposes several core functions to manage pages in memory and on disk:

1. `newPage(pagenum int64)`: This function checks the free list for an unused page. If no page is available, it evicts a page from the unpinned list and returns it as a clean page. This page is then initialized and returned for use.

2. `GetNewPage()`: This function allocates a new page with the next available page number, initializes it, and returns it to the caller. This function ensures that the new page is correctly set up in memory, but it does not set the page as dirty by default.

3. `GetPage(pagenum int64)`: This function retrieves a page given a specific page number. If the page is already in memory, it checks the pin count and either pins or returns the page. If the page is not in memory, it reads the page from disk and updates the page table before returning it.

4. `FlushPage(pageFrame *PageFrame)`: This function writes the data of a page to the disk if it is dirty. It ensures that only modified pages are written to disk to optimize I/O operations.

5. `FlushAllPages()`: This function iterates through all pages in memory (both pinned and unpinned lists) and flushes them to disk if they are dirty. This ensures that all changes made in memory are persisted before the application shuts down or before a new operation begins.

### Buffer Management and LRU Eviction
The pager maintains a buffer cache to optimize access to frequently used pages. The LRU cache eviction strategy ensures that when the pager runs out of available pages in the buffer, the least recently used page is evicted to make room for a new one. The implementation of this eviction strategy is facilitated by maintaining the unpinned list, where pages that are not currently pinned are placed in a queue. When space is needed, the pager evicts pages from this list in a manner that respects the LRU order.

The pager's page table ensures that when a page is fetched from disk, it is placed in the appropriate frame in the buffer and is registered in the page table. The pageTable structure, implemented in pager.go, is a critical component for efficiently managing the mapping between virtual memory (in the form of page IDs) and physical memory (frames in the buffer).

### Handling Dirty Pages
A dirty page is one that has been modified in memory but not yet written back to disk. The pager tracks dirty pages and ensures that they are flushed to disk before they are evicted. The flushing of dirty pages is critical for maintaining consistency between the in-memory state and the persisted state on disk.

## B+ Tree Indexer

The B+ Tree optimizes both search and data retrieval operations. Unlike binary search trees (BST), the B+ Tree generalizes the concept to allow nodes with more than two children, resulting in better performance for large datasets. This subsection provides a comprehensive explanation of how the B+ Tree is structured and how its insertion and splitting mechanisms are implemented in this project.

### B+ Tree Structure
In the context of this project, the B+ Tree is implemented with specific properties and operations that distinguish it from a regular B-Tree. A B+ Tree adheres to the following key principles:

1. **Internal Nodes and Leaf Nodes:**
  - Internal Nodes: These nodes contain only keys, which are used for searching. Internal nodes do not store actual data but instead point to child nodes, which are either internal or leaf nodes.
  - Leaf Nodes: Unlike internal nodes, leaf nodes store actual data values and point to their right neighbor, facilitating fast linear scans through the tree.

2. **Order of Nodes:**

  - Each node can have a maximum of _m_ children, and every non-root internal node must have at least _⌈m/2⌉_ children.
  - All leaves appear at the same level, which allows for efficient access and uniformity in node traversal.

3. **Splitting and Insertion:**

  - Insertion: When inserting a new key into a leaf node, if the node is full, it is split. The median key from the split node is then pushed up into the parent node. If the parent node is also full, it too will split, and this process may propagate up to the root.
  - Splitting: The process of splitting a node is crucial for maintaining the balance of the B+ Tree. When a node is full, the node is split into two equal parts, and the median key is propagated upward. This ensures that the B+ Tree retains its properties of balanced growth.

4. Search and Query:

Search operations start from the root and traverse the tree using binary search until the correct leaf node is found. Once the leaf node is reached, the desired key is searched using a second binary search.

### Relevant Files for B+ Tree Implementation
In this project, the B+ Tree is implemented across several key files. Each of these files handles different aspects of the tree’s functionality:

1. `pkg/btree/leafNode.go`: This file contains the functions related to leaf nodes. Key functions include:

  - `insert(key int64, value int64, update bool)`: This function inserts a key-value pair into the leaf node. If the node becomes full after insertion, it will split, and the Split struct will be returned to indicate the split operation.
  - `split()`: This function splits a leaf node when it becomes full. The median key is pushed to the parent node, and the data is redistributed between the original and the new leaf node.

2. `pkg/btree/internalNode.go`: Internal nodes are managed here. Important functions in this file are:

  - `insertSplit(split Split)`: Handles the insertion of a new key from a child node's split into an internal node.
  - `split()`: Splits an internal node when it becomes full and propagates the median key to the parent.

3. `pkg/btree/btree.go`: This file manages the overall B+ Tree structure and its operations. It contains:

  - `Select()`: This function retrieves a set of entries from the tree. It relies on the cursor abstraction to traverse through leaf nodes efficiently.
  - `SelectRange(startKey int64, endKey int64)`: Facilitates range queries by scanning through the leaf nodes between the specified start and end keys.

### Key Operations and Functions
1. **Node Insertion:** The insertion process begins with placing the key in the correct position in a leaf node. If the node overflows, it is split, and the median key is passed up to the parent. This cascading split process ensures that the tree remains balanced. Functions such as `insert()` and `insertSplit()` in the `leafNode.go` and `internalNode.go` files, respectively, handle these operations.

2. **Node Splitting:** Splitting a node occurs when the number of keys exceeds the capacity of the node. The `split()` function in both the leaf and internal node files handles this by dividing the node’s entries into two parts, pushing the median key upwards. The newly created node is then linked to the original one via sibling pointers in leaf nodes.

3. **Cursor Operations:** The B+ Tree uses a cursor abstraction to optimize linear scans. A cursor points to a specific entry in a leaf node, and it can move to the next entry in the sequence using the `Next()` function. This simplifies traversing through the tree for range queries and improves the efficiency of data retrieval. The cursor.go file implements functions like `CursorAtStart()` and `Next()`, which allow seamless movement through the leaf nodes.


## Hashing
The hashing component of this project involves implementing an **_extendible hash table_**, which is a dynamic hashing scheme designed for efficient key-value storage and retrieval. It allows for adaptive resizing to handle increased data volume while maintaining efficient operations. The implementation spans across multiple files, primarily focusing on `pkg/hash/bucket.go` and `pkg/hash/hashTable.go`, and involves core operations such as insertion, splitting, and data retrieval.

### Key Files and Functions

1. `pkg/hash/bucket.go':

This file defines the HashBucket struct and associated operations. The main function to implement here is:

`Insert(key int64, value int64) bool`:
This function inserts a key-value pair into the bucket. If the bucket exceeds its maximum capacity (`MAX_BUCKET_SIZE`), it signals the need for a split by returning true. The helper functions `modifyEntry` and `updateNumKeys` are used to manage entries efficiently.

Other operations such as `Find`, `Update`, `Delete`, and `Select` are implemented to support bucket-level interactions.

2. pkg/hash/hashTable.go
This file contains the `HashTable` struct and its critical methods:

- `split(bucket *HashBucket, hash int64) error`:
This function handles bucket splitting by creating new buckets, redistributing entries, and updating pointers in the hash table. When splitting, it ensures that the bucket depth increments correctly and remains consistent with the global depth.

- `Insert(key int64, value int64) error`:
This function performs key-value insertion, managing bucket overflow and triggering splitting as needed. It leverages the `Hasher` function from `pkg/hash/hashers.go` for consistent hash computations.

- `Select() ([]utils.Entry, error)`:
This function retrieves all entries across the hash table, enabling comprehensive data retrieval.

### Hashing and Splitting Mechanism
The extendible hashing mechanism uses the last `d` bits of the hash (global depth) to index into the hash table. Each bucket has a local depth (`di`), representing the number of significant bits it uses for addressing. When a bucket overflows

1. The bucket's local depth is incremented.
2. A new bucket is created, and entries are redistributed based on the updated hash bits.
3. If the local depth exceeds the global depth, the table size doubles, and all pointers are updated.

Bucket splitting ensures uniform distribution of keys, reducing the likelihood of future collisions.

## Joins
The Join feature introduces advanced relational database functionality, allowing the combination of tables to extract meaningful insights. This is implemented through the grace hash join algorithm and is further optimized using bloom filters.

### Key Files
1. `pkg/join/hash_join.go`: Implements the core grace hash join logic.
2. `pkg/join/bloom_filter.go`: Implements bloom filters for optimizing bucket probing.
3. `pkg/join/join_repl.go`: Integrates join functionality with the REPL interface, adding the `join` command.

### Grace Hash Join
Grace Hash Join is implemented to handle joins efficiently even when the hash table size exceeds memory capacity. It partitions the input tables into smaller, manageable buckets using a consistent hash function. Each bucket pair is then joined in-memory.

#### Key Functions in hash_join.go:
- `buildHashIndex`: Constructs hash indexes for input tables. This uses `db.Index` and iterates through records with a `Cursor`. Temporary transformations between keys and values ensure compatibility with varying join conditions.
- `probeBuckets`: Handles the probing of matching records between bucket pairs. Concurrent execution is enabled using Go's goroutines and channels. The `sendResult` helper function streams `EntryPair` results, supporting seamless concurrent processing.

### Bloom Filters
Bloom filters are integrated to reduce unnecessary bucket scans by filtering out values that are definitely not present in a bucket.

#### Key Functions in `bloom_filter.go`:
- `CreateFilter`: Initializes a Bloom Filter with a bitset and predefined hash functions (`hash.XxHasher`, `hash.MurmurHasher`).
- `Insert`: Hashes input keys and sets corresponding bits in the bitset.
- `Contains`: Checks if a key might be present by verifying the hashed bit positions.

The Bloom Filter is used in `probeBuckets` to skip scanning for keys that are guaranteed absent, significantly improving join performance for large datasets.

## Concurrency
The concurrency feature in Database is implemented to allowed multiple transactions to execute simultaneously while preserving data integrity and consistency. This feature ensures the database adheres to the ACID properties, particularly isolation, despite concurrent access by multiple clients. The implementation spans several components in the project, primarily located within the `pkg/concurrency` directory.

### Key Components and Files
1. `pkg/concurrency/concurrency_manager.go`
The `ConcurrencyManager` orchestrates locking and transaction management. It ensures that conflicting operations on the same data are serialized to avoid race conditions and deadlocks. It employs a two-phase locking protocol:
- **Phase 1: Growing Phase**: Locks are acquired but not released.
- **Phase 2: Shrinking Phase**: - Locks are released and cannot be reacquired. The file defines methods such as:
  - `AcquireLock(clientId uuid.UUID, lockType LockType, resource ResourceId)`: Acquires a lock for a given transaction and resource. The lock type can be read or write.
  - `ReleaseLocks(clientId uuid.UUID)`: Releases all locks held by a transaction, invoked during transaction commit or rollback.
  - `DetectDeadlock()`: Implements deadlock detection using a waits-for graph.

2. `pkg/concurrency/lock_table.go`
This file implements the LockTable, which maps resources to their associated locks. It maintains a queue of lock requests for each resource, ensuring fair scheduling and adherence to locking policies.

Important methods include:
- `RequestLock(resource ResourceId, clientId uuid.UUID, lockType LockType)`: Enqueues a lock request and grants it if no conflicting locks exist.
- `ReleaseLock(resource ResourceId, clientId uuid.UUID)`: Removes a lock from the table and potentially grants queued locks.

3. `pkg/concurrency/transaction_manager.go`
This module manages the lifecycle of transactions, including their states (`Active`, `Committed`, `Aborted`) and metadata. It interacts closely with the `ConcurrencyManager` and `RecoveryManager` to ensure transactional consistency even in the presence of crashes.

The core methods include:
- `BeginTransaction(clientId uuid.UUID)`: Initializes a new transaction and assigns it a unique ID.
- `CommitTransaction(clientId uuid.UUID)`: Signals the end of a transaction, triggering lock release and logging a commit entry.
- `AbortTransaction(clientId uuid.UUID)`: Rolls back a transaction by undoing all its operations and releasing associated locks.

4. `pkg/concurrency/tests`
This directory contains unit and integration tests that validate the concurrency model under various scenarios

### Concurrency Model
The concurrency implementation follows a strict two-phase locking protocol with additional optimizations:

- **Shared and Exclusive Locks**: Supports shared locks for read operations and exclusive locks for write operations. This granularity minimizes contention and maximizes parallelism.
- **Deadlock Detection**: Periodic checks analyze the waits-for graph to identify cycles, terminating one or more transactions to break deadlocks.
- **Fair Scheduling**: Ensures lock requests are processed in a FIFO manner, preventing starvation of transactions.

### Integration with Recovery
The concurrency and recovery modules are tightly coupled. For instance:

- **Abort Mechanism**: During a transaction abort, the ConcurrencyManager ensures that all locks are released, and the RecoveryManager undoes uncommitted changes.
- **Checkpointing**: Locks are temporarily escalated during checkpoints to ensure consistency between memory and disk states.

### Challenges and Considerations
The design addresses potential challenges such as:

- **Deadlocks**: Handled gracefully through detection and resolution, with minimal transaction rollbacks.
- **Performance**: Efficient lock acquisition and release ensure high throughput under concurrent workloads.
- **Crash Recovery**: Concurrency operations are logged to enable proper recovery in the event of a crash, preserving transactional consistency.

## Recovery
The recovery feature is the cornerstone of crash tolerance in the database, ensuring consistency and durability despite unexpected failures. This section details the implementation, structured into logging, rollback, and recovery mechanisms, with references to relevant files in the pkg/recovery package. This feature builds on Write-Ahead Logging (WAL) and employs checkpoints for efficient restoration.

### Logging
The logging mechanism is central to recovery. It tracks changes to the database, ensuring every transaction can be replayed or undone as necessary. Key functions to implement in `pkg/recovery/recovery_manager.go` include:

1. Edit:

`func (rm *RecoveryManager) Edit(clientId uuid.UUID, table db.Index, action Action, key int64, oldval int64, newval int64)`

This function writes an EDIT log before applying any modification. Each log follows the structure:

`<Tx, table, INSERT|DELETE|UPDATE, key, oldval, newval>`
Logs are appended using `rm.flushLog(log)`. Active transactions are tracked in `txStack`, mapping transaction IDs to their respective logs.

2. Start and Commit:

`func (rm *RecoveryManager) Start(clientId uuid.UUID)`
`func (rm *RecoveryManager) Commit(clientId uuid.UUID)`

- Start initializes a transaction, logging `<Tx start>`.
- Commit finalizes the transaction, logging `<Tx commit>` and clearing its `txStack` entry.

3. Checkpoint:

`func (rm *RecoveryManager) Checkpoint()`
- Flushes all in-memory pages to disk using:

```
for table in rm.db.GetTables() {
    table.GetPager().LockAllPages()
    table.GetPager().FlushAllPages()
    table.GetPager().UnlockAllPages()
}
```
- Writes a `<checkpoint>` log containing active transactions.

The file `pkg/recovery/log.go` handles log serialization and deserialization, providing utility functions to format and parse logs.

### Rollback
Rollback ensures that incomplete or aborted transactions leave no lasting impact on the database. This is implemented in:

`func (rm *RecoveryManager) Rollback(clientId uuid.UUID) error`

Rollback iterates backward through the `txStack` logs of a given transaction, calling `undo()` for each operation. It maintains consistency by:

1. Reversing changes without compromising committed data.
2. Logging the rollback's completion to the RecoveryManager and TransactionManager.

### Recovery Mechanism
The recovery mechanism restores the database to a consistent state following a crash. Implemented in:

`func (rm *RecoveryManager) Recover() error`

The recovery algorithm follows these steps:

1. **Checkpoint Discovery:** Retrieve logs and the most recent checkpoint using rm.readLogs(). The checkpoint log specifies transactions active at the time.

2. **Redo Phase:** Replay logs from the checkpoint onward, applying changes for committed transactions. Use the `redo()` function for this purpose.

3. **Undo Phase:** Identify uncommitted transactions and reverse their operations using undo().

Recovery also incorporates **Prime** and **Delta** functions to maintain database integrity, simulating a copy-on-write structure. These ensure that recovery begins from a clean snapshot, avoiding corruption due to partial flushes.
