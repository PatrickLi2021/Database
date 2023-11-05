package concurrency

import (
	"errors"
	"sync"

	db "github.com/csci1270-fall-2023/dbms-projects-handout/pkg/db"
	uuid "github.com/google/uuid"
)

// To detect cycles, run DFS on every node
// Each client can have a transaction running. Each transaction has a list of locked resources.
type Transaction struct {
	clientId  uuid.UUID
	resources map[Resource]LockType
	lock      sync.RWMutex
}

// Grab a write lock on the tx
func (t *Transaction) WLock() {
	t.lock.Lock()
}

// Release the write lock on the transaction
func (t *Transaction) WUnlock() {
	t.lock.Unlock()
}

// Grab a read lock on the tx
func (t *Transaction) RLock() {
	t.lock.RLock()
}

// Release the write lock on the tx
func (t *Transaction) RUnlock() {
	t.lock.RUnlock()
}

// Get the transaction id.
func (t *Transaction) GetClientID() uuid.UUID {
	return t.clientId
}

// Get the transaction's resources.
func (t *Transaction) GetResources() map[Resource]LockType {
	return t.resources
}

// Transaction Manager manages all of the transactions on a server.
type TransactionManager struct {
	lm           *LockManager
	tmMtx        sync.RWMutex
	pGraph       *Graph
	transactions map[uuid.UUID]*Transaction
}

// Get a pointer to a new transaction manager.
func NewTransactionManager(lm *LockManager) *TransactionManager {
	return &TransactionManager{lm: lm, pGraph: NewGraph(), transactions: make(map[uuid.UUID]*Transaction)}
}

// Get the transactions.
func (tm *TransactionManager) GetLockManager() *LockManager {
	return tm.lm
}

// Get the transactions.
func (tm *TransactionManager) GetTransactions() map[uuid.UUID]*Transaction {
	return tm.transactions
}

// Get a particular transaction.
func (tm *TransactionManager) GetTransaction(clientId uuid.UUID) (*Transaction, bool) {
	tm.tmMtx.RLock()
	defer tm.tmMtx.RUnlock()
	t, found := tm.transactions[clientId]
	return t, found
}

// Begin a transaction for the given client; error if already began.
func (tm *TransactionManager) Begin(clientId uuid.UUID) error {
	tm.tmMtx.Lock()
	defer tm.tmMtx.Unlock()
	_, found := tm.transactions[clientId]
	if found {
		return errors.New("transaction already began")
	}
	tm.transactions[clientId] = &Transaction{clientId: clientId, resources: make(map[Resource]LockType)}
	return nil
}

// Locks the given resource. Will return an error if deadlock is created.
func (tm *TransactionManager) Lock(clientId uuid.UUID, table db.Index, resourceKey int64, lType LockType) error {
	// Read lock tmMtx outside of getTransaction
	tm.tmMtx.RLock()
	defer tm.tmMtx.RUnlock()
	// Fetch transaction by UUID and lock it, since we will be getting resources
	transaction, found_status := tm.GetTransaction(clientId)
	transaction.lock.RLock()
	// If the transaction is found, lock the resource
	if found_status {
		map_of_resources := transaction.GetResources()
		for resource := range map_of_resources {
			// Checking if we have rights to resource (resource is in map_of_resources)
			if resource.resourceKey == resourceKey {
				// Find what lock we have
				current_lock_type := map_of_resources[resource]
				// Currently hold RLock and request RLock again
				if current_lock_type == 0 && lType == R_LOCK {
					return nil
			
				// Currently hold RLock and request WLock
				} else if current_lock_type == 0 && lType == W_LOCK {
					return errors.New("cannot upgrade lock type")

				// Currently hold WLock
				} else if current_lock_type == 1 {
					return nil
				}
				// Look for conflicting transactions
				conflicting_transactions := tm.discoverTransactions(resource, current_lock_type)
				// If conflicting transactions are found, add edges to precedence graph
					for _, conflicting_transaction := range conflicting_transactions {
						tm.pGraph.AddEdge(transaction, conflicting_transaction)
					}
				if tm.pGraph.DetectCycle() {
					return errors.New("cycle detected")
				}
				// Add resource to transaction's resource list and lock it
				transaction.resources[resource] = lType
				tm.lm.Lock(resource, lType)
			}
		}
	}
	return errors.New("transaction was not found")
}

// Unlocks the given resource.
func (tm *TransactionManager) Unlock(clientId uuid.UUID, table db.Index, resourceKey int64, lType LockType) error {
	// Read lock tmMtx outside of getTransaction
	tm.tmMtx.RLock()
	defer tm.tmMtx.RUnlock()
	// Fetch transaction by UUID and lock it, since we will be getting resources
	transaction, found_status := tm.GetTransaction(clientId)
	if found_status {
		transaction.RLock()
		defer transaction.RUnlock()
		map_of_resources := transaction.GetResources()
		resource := Resource{tableName: table.GetName(), resourceKey: resourceKey}
		resource_lock_type, found := map_of_resources[resource]
		if !found {
			return errors.New("resource could not be found")
		}
		if resource_lock_type != lType {
			return errors.New("lock type mismatch")
		}
		delete(map_of_resources, resource)
		tm.lm.Unlock(resource, lType)
		return nil
	}
	return errors.New("transaction was not found")
}

// Commits the given transaction and removes it from the running transactions list.
func (tm *TransactionManager) Commit(clientId uuid.UUID) error {
	tm.tmMtx.Lock()
	defer tm.tmMtx.Unlock()
	// Get the transaction we want.
	t, found := tm.transactions[clientId]
	if !found {
		return errors.New("no transactions running")
	}
	// Unlock all resources.
	t.RLock()
	defer t.RUnlock()
	for r, lType := range t.resources {
		err := tm.lm.Unlock(r, lType)
		if err != nil {
			return err
		}
	}
	// Remove the transaction from our transactions list.
	delete(tm.transactions, clientId)
	return nil
}

// Returns a slice of all transactions that conflict w/ the given resource and locktype.
func (tm *TransactionManager) discoverTransactions(r Resource, lType LockType) []*Transaction {
	ret := make([]*Transaction, 0)
	for _, t := range tm.transactions {
		t.RLock()
		for storedResource, storedType := range t.resources {
			if storedResource == r && (storedType == W_LOCK || lType == W_LOCK) {
				ret = append(ret, t)
				break
			}
		}
		t.RUnlock()
	}
	return ret
}
