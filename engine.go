package kvs

import (
	"errors"
	"time"
)

// Common errors returned by the engine implementations.
var (
	// ErrEngineAlreadyClosed is returned when attempting to do an operation on the engine after
	// it has been closed.
	ErrEngineAlreadyClosed = errors.New("engine is already closed")

	// ErrTxReadOnly is returned when attempting to call write methods on a read-only
	// transaction.
	ErrTxReadOnly = errors.New("transaction is read-only")

	// ErrTxAlreadyCompleted is returned when attempting to commit or rollback a transaction
	// already committed or rolled back.
	ErrTxAlreadyCompleted = errors.New("transaction has already completed")

	// ErrKeyNotFound is returned when the targeted key doesn't exist.
	ErrKeyNotFound = errors.New("key not found")
)

// Engine is responsible for storing data as key value pairs. Implementations can choose to store
// data anywhere they want.
//
// Engines must support read-only and read-write transactions.
type Engine interface {
	// NewTransaction returns a read-only or read-write transaction depending on whether writable
	// is set to false or true, respectively. If the engine is already closed, the
	// ErrEngineAlreadyClosed error is returned.
	//
	// When NewTransaction() is called, it is absolutely essential to call Discard() via defer on
	// the returned transaction. This should be done irrespective of what writable is set to, or
	// whether Commit() will be called on the transaction.
	//
	//   txn, err := engine.NewTransaction(false)
	//   if err != nil {
	//     return err
	//   }
	//   defer txn.Discard()
	//   // Do operation using the transaction...
	//
	// The behaviour of opening a transaction when another one is already open depends on the
	// implementation.
	NewTransaction(writable bool) (Transaction, error)

	// Close the engine and stop creating any new transactions or committing changes. If the engine
	// is already closed, this method is a no-op.
	Close() error
}

// Transaction provides methods for managing key value pairs and the transaction itself.
//
// The transaction is either read-only or read-write. Read-only transactions can be used to read
// data and read-write ones can be used to read, create, delete and modify data. If the
// transaction is read-only, any call to a write method must return the ErrTxReadOnly error.
type Transaction interface {
	// Commit commits the transaction and any change made during its lifetime. If the transaction
	// has already rolled back or committed, the ErrTxAlreadyCompleted error is returned. If the
	// engine is already closed, the ErrEngineAlreadyClosed error is returned.
	Commit() error

	// Discard discards a transaction and cancels any change made during its lifetime.
	//
	// This method is critical and must be called via a defer just after a new transaction is
	// created, irrespective of whether the transaction is read-only or read-write. Calling this
	// method multiple times or after Commit doesn't cause any issues.
	Discard() error

	// Get returns the value associated with the given key. If no key is not found, the
	// ErrKeyNotFound error is returned.
	Get(k []byte) ([]byte, error)

	// Put stores a key value pair. If it already exists, the new value overrides the old one.
	Put(k, v []byte, options PutOptions) error

	// Delete a key value pair. If the key is not found, the ErrKeyNotFound error is returned.
	Delete(k []byte) error

	// Clear deletes all the key value pairs from the engine.
	Clear() error

	// Iterate looks for the pivot and then goes through all the subsequent key value pairs in
	// increasing order and calls the given function for each pair.
	//
	// If the given function returns an error, the iteration stops and returns that error.
	// If the pivot is nil, iteration starts from the beginning.
	Iterate(pivot []byte, fn func(k, v []byte) error) error

	// IterateReverse looks for the pivot and then goes through all the subsequent key value pairs
	// in decreasing order and calls the given function for each pair.
	//
	// If the given function returns an error, the iteration stops and returns that error.
	// If the pivot is nil, iteration starts from the end.
	IterateReverse(pivot []byte, fn func(k, v []byte) error) error
}

// PutOptions are specified while putting a new key value pair in the engine.
type PutOptions interface {
	// If TTL is > 0, the key expires after the specified duration.
	TTL() time.Duration
}
