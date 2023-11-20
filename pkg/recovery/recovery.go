package recovery

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"

	concurrency "github.com/csci1270-fall-2023/dbms-projects-handout/pkg/concurrency"
	db "github.com/csci1270-fall-2023/dbms-projects-handout/pkg/db"
	"github.com/otiai10/copy"

	uuid "github.com/google/uuid"
)

// Recovery Manager.
type RecoveryManager struct {
	d       *db.Database
	tm      *concurrency.TransactionManager
	txStack map[uuid.UUID]([]Log)
	fd      *os.File
	mtx     sync.Mutex
}

// Construct a recovery manager.
func NewRecoveryManager(
	d *db.Database,
	tm *concurrency.TransactionManager,
	logName string,
) (*RecoveryManager, error) {
	fd, err := os.OpenFile(logName, os.O_APPEND|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}
	return &RecoveryManager{
		d:       d,
		tm:      tm,
		txStack: make(map[uuid.UUID][]Log),
		fd:      fd,
	}, nil
}

// Write the string `s` to the log file. Expects rm.mtx to be locked
func (rm *RecoveryManager) writeToBuffer(s string) error {
	_, err := rm.fd.WriteString(s)
	if err != nil {
		return err
	}
	err = rm.fd.Sync()
	return err
}

// Write a Table log.
func (rm *RecoveryManager) Table(tblType string, tblName string) {
	rm.mtx.Lock()
	defer rm.mtx.Unlock()
	tl := tableLog{
		tblType: tblType,
		tblName: tblName,
	}
	rm.writeToBuffer(tl.toString())
}

// Write an Edit log.
func (rm *RecoveryManager) Edit(clientId uuid.UUID, table db.Index, action Action, key int64, oldval int64, newval int64) {
	// Lock recovery manager
	rm.mtx.Lock()
	defer rm.mtx.Unlock()
	// Create edit log and write it to the log file
	editl := editLog{
		id: clientId,
		tablename: table.GetName(),
		action: action,
		key: key,
		oldval: oldval,
		newval: newval,
	}
	rm.writeToBuffer(editl.toString())
	// Update correspongind txStack entry
	rm.txStack[clientId] = append(rm.txStack[clientId], Log(&editl))
}

// Write a transaction start log.
func (rm *RecoveryManager) Start(clientId uuid.UUID) {
	// Lock recovery manager
	rm.mtx.Lock()
	defer rm.mtx.Unlock()
	// Create start log and write it to the log file
	startl := startLog{
		id: clientId,
	}
	rm.writeToBuffer(startl.toString())
	// Make a entry for this transaction corresponding to the start log
	new_log_list := []Log{Log(&startl)}
	rm.txStack[clientId] = new_log_list
}

// Write a transaction commit log.
func (rm *RecoveryManager) Commit(clientId uuid.UUID) {
	// Lock the recovery manager
	rm.mtx.Lock()
	defer rm.mtx.Unlock()
	// Create commit log and write it to the log file
	commitl := commitLog{
		id: clientId,
	}
	rm.writeToBuffer(commitl.toString())
	// Delete all data in txStack map for this particular transaction
	delete(rm.txStack, clientId)
}

// Flush all pages to disk and write a checkpoint log.
func (rm *RecoveryManager) Checkpoint() {
	rm.mtx.Lock()
	defer rm.mtx.Unlock()
	// Lock all pages to prevent tables from being changed while making checkpointing
	all_tables := rm.d.GetTables()
	for _, table := range(all_tables) {
		table.GetPager().LockAllUpdates()
		table.GetPager().FlushAllPages()
	}
	// Create checkpoint log by getting all active transactions
	active_transaction_ids := make([]uuid.UUID, 0)
	for transaction_id := range rm.txStack {
		active_transaction_ids = append(active_transaction_ids, transaction_id)
	}
	checkpointl := checkpointLog{
		ids: active_transaction_ids,
	}
	rm.writeToBuffer(checkpointl.toString())

	// Unlock all table pages
	for _, table := range(all_tables) {
		table.GetPager().UnlockAllUpdates()
	}
	rm.Delta() // Sorta-semi-pseudo-copy-on-write (to ensure db recoverability)
}

// Redo a given log's action.
func (rm *RecoveryManager) Redo(log Log) error {
	switch log := log.(type) {
	case *tableLog:
		payload := fmt.Sprintf("create %s table %s", log.tblType, log.tblName)
		err := db.HandleCreateTable(rm.d, payload, os.Stdout)
		if err != nil {
			return err
		}
	case *editLog:
		switch log.action {
		case INSERT_ACTION:
			payload := fmt.Sprintf("insert %v %v into %s", log.key, log.newval, log.tablename)
			err := db.HandleInsert(rm.d, payload)
			if err != nil {
				// There is already an entry, try updating
				payload := fmt.Sprintf("update %s %v %v", log.tablename, log.key, log.newval)
				err = db.HandleUpdate(rm.d, payload)
				if err != nil {
					return err
				}
			}
		case UPDATE_ACTION:
			payload := fmt.Sprintf("update %s %v %v", log.tablename, log.key, log.newval)
			err := db.HandleUpdate(rm.d, payload)
			if err != nil {
				// Entry may have been deleted, try inserting
				payload := fmt.Sprintf("insert %v %v into %s", log.key, log.newval, log.tablename)
				err := db.HandleInsert(rm.d, payload)
				if err != nil {
					return err
				}
			}
		case DELETE_ACTION:
			payload := fmt.Sprintf("delete %v from %s", log.key, log.tablename)
			err := db.HandleDelete(rm.d, payload)
			if err != nil {
				return err
			}
		}
	default:
		return errors.New("can only redo edit logs")
	}
	return nil
}

// Undo a given log's action.
func (rm *RecoveryManager) Undo(log Log) error {
	switch log := log.(type) {
	case *editLog:
		switch log.action {
		case INSERT_ACTION:
			payload := fmt.Sprintf("delete %v from %s", log.key, log.tablename)
			err := HandleDelete(rm.d, rm.tm, rm, payload, log.id)
			if err != nil {
				return err
			}
		case UPDATE_ACTION:
			payload := fmt.Sprintf("update %s %v %v", log.tablename, log.key, log.oldval)
			err := HandleUpdate(rm.d, rm.tm, rm, payload, log.id)
			if err != nil {
				return err
			}
		case DELETE_ACTION:
			payload := fmt.Sprintf("insert %v %v into %s", log.key, log.oldval, log.tablename)
			err := HandleInsert(rm.d, rm.tm, rm, payload, log.id)
			if err != nil {
				return err
			}
		}
	default:
		return errors.New("can only undo edit logs")
	}
	return nil
}

// Do a full recovery to the most recent checkpoint on startup.
func (rm *RecoveryManager) Recover() error {
	panic("function not yet implemented")
}

// Roll back a particular transaction.
func (rm *RecoveryManager) Rollback(clientId uuid.UUID) error {
	list_of_logs := rm.txStack[clientId]
	// If list of logs is empty, commit then return
	if len(list_of_logs) == 0 {
		rm.Commit(clientId)
		return nil
	
	} else {
		// Check to see if the first of the logs is a start log
		first_log := list_of_logs[0]
		switch first_log.(type) {
		case *(startLog):
			// If log is well-formed, we want to delete from the log then
			for i := len(list_of_logs) - 1; i >= 0; i-- {
				switch current_log := list_of_logs[i].(type) {
				case *(editLog):
					rm.Undo(current_log)
				}
			}
			// Commit to transaction and recovery managers
			rm.Commit(clientId)
			rm.tm.Commit(clientId)
		default:
			return errors.New("First log was not a start log")
		}
	}
	// If there are logs, then we want to make sure that those logs are valid and well-formed. We only need to
	// check if the first of the logs is a start log to signify that we started a transaction. That's how we 
	// check if a log is valid. We then rollback the rest of the logs.
	// Commit to both teh RecoveryManager and Transactionmanager when done
	

	panic("function not yet implemented")
}

// Primes the database for recovery
func Prime(folder string) (*db.Database, error) {
	// Ensure folder is of the form */
	base := strings.TrimSuffix(folder, "/")
	recoveryFolder := base + "-recovery/"
	dbFolder := base + "/"
	if _, err := os.Stat(dbFolder); err != nil {
		if os.IsNotExist(err) {
			err := os.MkdirAll(recoveryFolder, 0775)
			if err != nil {
				return nil, err
			}
			return db.Open(dbFolder)
		}
		return nil, err
	}
	if _, err := os.Stat(recoveryFolder); err != nil {
		if os.IsNotExist(err) {
			return db.Open(dbFolder)
		}
		return nil, err
	}
	os.RemoveAll(dbFolder)
	err := copy.Copy(recoveryFolder, dbFolder)
	if err != nil {
		return nil, err
	}
	return db.Open(dbFolder)
}

// Should be called at end of Checkpoint.
func (rm *RecoveryManager) Delta() error {
	folder := strings.TrimSuffix(rm.d.GetBasePath(), "/")
	recoveryFolder := folder + "-recovery/"
	folder += "/"
	os.RemoveAll(recoveryFolder)
	err := copy.Copy(folder, recoveryFolder)
	return err
}
