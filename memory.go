package kvs

import (
	"bytes"
	"container/heap"
	"sync"
	"time"

	"github.com/google/btree"
)

type engine struct {
	tree     *btree.BTree
	expiries *minHeap

	closeCh chan struct{}
	mu      sync.RWMutex
}

type transaction struct {
	e        *engine
	writable bool

	completed  bool
	onRollback []func()
	onCommit   []func()
}

// NewInMemoryEngine creates a new in-memory engine.
func NewInMemoryEngine() Engine {
	e := &engine{
		tree:     btree.New(3),
		expiries: newMinHeap(),
		closeCh:  make(chan struct{}),
	}

	go e.removeExpiredWorker()
	return e
}

func (e *engine) NewTransaction(writable bool) (Transaction, error) {
	if writable {
		e.mu.Lock()
	} else {
		e.mu.RLock()
	}

	if e.closed() {
		return nil, ErrEngineAlreadyClosed
	}

	return &transaction{e: e, writable: writable}, nil
}

func (e *engine) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.closed() {
		return nil
	}

	close(e.closeCh)
	e.tree, e.expiries = nil, nil
	return nil
}

func (e *engine) closed() bool {
	select {
	case <-e.closeCh:
		return true
	default:
		return false
	}
}

func (tx *transaction) Commit() error {
	if tx.e.closed() {
		return ErrEngineAlreadyClosed
	}
	if tx.completed {
		return ErrTxAlreadyCompleted
	}

	for _, fn := range tx.onCommit {
		fn()
	}

	tx.close()
	return nil
}

func (tx *transaction) Discard() error {
	if tx.e.closed() || tx.completed {
		return nil
	}

	for _, fn := range tx.onRollback {
		fn()
	}

	tx.close()
	return nil
}

func (tx *transaction) close() {
	tx.completed = true
	if tx.writable {
		tx.e.mu.Unlock()
	} else {
		tx.e.mu.RUnlock()
	}

	tx.onRollback = nil
	tx.onCommit = nil
}

type item struct {
	k, v    []byte
	deleted bool

	expiry time.Time
	index  int
}

func (it *item) Less(than btree.Item) bool {
	return bytes.Compare(it.k, than.(*item).k) < 0
}

func (it *item) inactive(now time.Time) bool {
	return it.deleted || (!it.expiry.IsZero() && now.After(it.expiry))
}

func (tx *transaction) Get(k []byte) ([]byte, error) {
	i := tx.e.tree.Get(&item{k: k})
	if i == nil {
		return nil, ErrKeyNotFound
	}

	it := i.(*item)
	if it.inactive(time.Now()) {
		return nil, ErrKeyNotFound
	}
	return it.v, nil
}

func (tx *transaction) Put(k, v []byte, options PutOptions) error {
	if !tx.writable {
		return ErrTxReadOnly
	}

	if k == nil {
		k = []byte{}
	}

	var expiry time.Time
	if options != nil {
		ttl := options.TTL()
		if ttl > 0 {
			expiry = time.Now().Add(ttl)
		}
	}

	it := &item{k: k}
	if i := tx.e.tree.Get(it); i != nil {
		cur := i.(*item)
		oldValue, oldDeleted, oldExpiry := cur.v, cur.deleted, cur.expiry
		cur.v = v
		cur.deleted = false
		cur.expiry = expiry
		tx.e.expiries.FixItem(cur)

		tx.onRollback = append(tx.onRollback, func() {
			cur.v = oldValue
			cur.deleted = oldDeleted
			cur.expiry = oldExpiry
			tx.e.expiries.FixItem(cur)
		})
		return nil
	}

	it.v = v
	it.expiry = expiry
	tx.e.tree.ReplaceOrInsert(it)
	tx.e.expiries.PushItem(it)

	tx.onRollback = append(tx.onRollback, func() {
		tx.e.tree.Delete(it)
		tx.e.expiries.RemoveItem(it)
	})
	return nil
}

func (tx *transaction) Delete(k []byte) error {
	if !tx.writable {
		return ErrTxReadOnly
	}

	i := tx.e.tree.Get(&item{k: k})
	if i == nil {
		return ErrKeyNotFound
	}

	it := i.(*item)
	if it.inactive(time.Now()) {
		return ErrKeyNotFound
	}

	it.deleted = true
	tx.onRollback = append(tx.onRollback, func() {
		it.deleted = false
	})
	tx.onCommit = append(tx.onCommit, func() {
		tx.e.tree.Delete(it)
		tx.e.expiries.RemoveItem(it)
	})
	return nil
}

func (tx *transaction) Clear() error {
	if !tx.writable {
		return ErrTxReadOnly
	}

	oldTree := tx.e.tree
	oldExpiries := tx.e.expiries
	tx.e.tree = btree.New(3)
	tx.e.expiries = newMinHeap()
	tx.onRollback = append(tx.onRollback, func() {
		tx.e.tree = oldTree
		tx.e.expiries = oldExpiries
	})
	return nil
}

func (tx *transaction) Iterate(pivot []byte, fn func(k, v []byte) error) error {
	var err error
	now := time.Now()
	iterator := btree.ItemIterator(func(it btree.Item) bool {
		i := it.(*item)
		if i.inactive(now) {
			return true
		}

		err = fn(i.k, i.v)
		return err == nil
	})

	if len(pivot) == 0 {
		tx.e.tree.Ascend(iterator)
	} else {
		tx.e.tree.AscendGreaterOrEqual(&item{k: pivot}, iterator)
	}
	return err
}

func (tx *transaction) IterateReverse(pivot []byte, fn func(k, v []byte) error) error {
	var err error
	now := time.Now()
	iterator := btree.ItemIterator(func(it btree.Item) bool {
		i := it.(*item)
		if i.inactive(now) {
			return true
		}

		err = fn(i.k, i.v)
		return err == nil
	})

	if len(pivot) == 0 {
		tx.e.tree.Descend(iterator)
	} else {
		tx.e.tree.DescendLessOrEqual(&item{k: pivot}, iterator)
	}
	return err
}

func (e *engine) removeExpiredWorker() {
	now := time.Now()
	t := time.NewTicker(time.Second)
	defer t.Stop()

	for {
		select {
		case <-e.closeCh:
			return
		case <-t.C:
			if e.hasExpired(now) {
				e.removeExpired(now)
			}
		}
	}
}

func (e *engine) hasExpired(now time.Time) bool {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if e.expiries.Len() == 0 {
		return false
	}

	it := e.expiries.PeekItem()
	return now.After(it.expiry)
}

func (e *engine) removeExpired(now time.Time) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	for {
		if e.expiries.Len() == 0 {
			break
		}

		it := e.expiries.PeekItem()
		if it.expiry.After(now) {
			break
		}

		e.expiries.PopItem()
		e.tree.Delete(it)
	}
}

// minHeap implements heap.Interface and holds backend items.
type minHeap []*item

// newMinHeap returns a new min heap.
func newMinHeap() *minHeap {
	mh := &minHeap{}
	heap.Init(mh)
	return mh
}

func (mh minHeap) Len() int {
	return len(mh)
}

func (mh minHeap) Less(i, j int) bool {
	return mh[i].expiry.Unix() < mh[j].expiry.Unix()
}

func (mh minHeap) Swap(i, j int) {
	mh[i], mh[j] = mh[j], mh[i]
	mh[i].index = i
	mh[j].index = j
}

func (mh *minHeap) Push(x interface{}) {
	n := len(*mh)
	it := x.(*item)
	it.index = n
	*mh = append(*mh, it)
}

func (mh *minHeap) Pop() interface{} {
	old := *mh
	n := len(old)
	it := old[n-1]
	it.index = -1
	*mh = old[0 : n-1]
	return it
}

func (mh *minHeap) PushItem(it *item) {
	if !it.expiry.IsZero() {
		heap.Push(mh, it)
	}
}

func (mh *minHeap) PopItem() *item {
	it := heap.Pop(mh)
	if it == nil {
		return nil
	}
	return it.(*item)
}

func (mh *minHeap) PeekItem() *item {
	items := *mh
	if len(items) == 0 {
		return nil
	}
	return items[0]
}

func (mh *minHeap) RemoveItem(it *item) {
	if !it.expiry.IsZero() {
		heap.Remove(mh, it.index)
	}
}

func (mh *minHeap) FixItem(it *item) {
	if !it.expiry.IsZero() {
		heap.Fix(mh, it.index)
	}
}
