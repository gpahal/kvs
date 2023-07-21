package kvs_test

import (
	"encoding/binary"
	"sync"
	"testing"

	"github.com/gpahal/kvs"
)

const (
	TOTAL_TRANSACTIONS    = 1000
	INCS_PER_TRANSACTIONS = 20
	FINAL_VALUE           = TOTAL_TRANSACTIONS * INCS_PER_TRANSACTIONS
)

func TestMemoryEngine(t *testing.T) {
	e := kvs.NewInMemoryEngine()

	var wg sync.WaitGroup
	keys := []string{"k1", "k2", "k3", "k4", "k5"}
	for i := 0; i < TOTAL_TRANSACTIONS; i++ {
		wg.Add(1)
		go increment100Times(t, &wg, e, keys)
	}

	wg.Wait()
	tx, err := e.NewTransaction(false)
	assertErrNil(t, err)
	defer tx.Discard()

	for _, key := range keys {
		bs, err := tx.Get([]byte(key))
		assertErrNil(t, err)

		val := bytesToInt32(bs)
		if val != FINAL_VALUE {
			t.Errorf("expecting value of key %s to be %d, got %d", key, TOTAL_TRANSACTIONS, val)
		}
	}
}

func increment100Times(t *testing.T, wg *sync.WaitGroup, e kvs.Engine, keys []string) {
	t.Helper()
	defer wg.Done()

	tx, err := e.NewTransaction(true)
	assertErrNil(t, err)
	defer tx.Discard()

	for i := 0; i < INCS_PER_TRANSACTIONS; i++ {
		for _, key := range keys {
			increment(t, tx, key)
		}
	}

	err = tx.Commit()
	assertErrNil(t, err)
}

func increment(t *testing.T, tx kvs.Transaction, key string) {
	t.Helper()
	bs, err := tx.Get([]byte(key))
	if err == kvs.ErrKeyNotFound {
		err := tx.Put([]byte(key), int32ToBytes(1), nil)
		assertErrNil(t, err)
		return
	}

	assertErrNil(t, err)
	val := bytesToInt32(bs)
	err = tx.Put([]byte(key), int32ToBytes(val+1), nil)
	assertErrNil(t, err)
}

func int32ToBytes(i uint32) []byte {
	bs := make([]byte, 4)
	binary.LittleEndian.PutUint32(bs, i)
	return bs
}

func bytesToInt32(bs []byte) uint32 {
	return binary.LittleEndian.Uint32(bs)
}

func assertErrNil(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("expecting err to be nil, got %s", err)
	}
}
