/*
Copyright 2025 The Aibrix Team.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package cache

import (
	"context"
	"sync"
	"time"

	"github.com/vllm-project/aibrix/pkg/utils"
)

type getCurrentTime func() time.Time

var DefaultGetCurrentTime = func() time.Time {
	return time.Now()
}

// TODO: make LRUStore thread-safe
type LRUStore[K comparable, V any] struct {
	sync.RWMutex
	freeTable map[K]*entry[K, V]
	lruList   *list[K, V]
	cap       int

	getCurrentTime
	interval  time.Duration
	ttl       time.Duration
	stop      chan struct{}
	lifetime  context.Context
	cancel    context.CancelFunc
	done      chan struct{}
	closeOnce sync.Once
}

func NewLRUStore[K comparable, V any](cap int, ttl, interval time.Duration, f getCurrentTime) *LRUStore[K, V] {
	return NewLRUStoreWithContext[K, V](context.Background(), cap, ttl, interval, f)
}

// NewLRUStoreWithContext owns eviction under lifetime. Close cancels and waits
// for eviction; entries remain usable afterward through the legacy methods.
func NewLRUStoreWithContext[K comparable, V any](lifetime context.Context, cap int, ttl, interval time.Duration, f getCurrentTime) *LRUStore[K, V] {
	lifetime, cancel := context.WithCancel(lifetime)
	store := &LRUStore[K, V]{
		freeTable:      make(map[K]*entry[K, V]),
		lruList:        &list[K, V]{head: &entry[K, V]{}, tail: &entry[K, V]{}},
		cap:            cap,
		ttl:            ttl,
		interval:       interval,
		getCurrentTime: f,
		stop:           make(chan struct{}),
		lifetime:       lifetime,
		cancel:         cancel,
		done:           make(chan struct{}),
	}
	store.lruList.head.next = store.lruList.tail
	store.lruList.tail.prev = store.lruList.head

	// A non-positive interval disables background eviction.
	if interval > 0 {
		go store.startEviction()
	} else {
		close(store.done)

	}
	return store
}

// Close idempotently cancels eviction and waits for its worker to exit.
func (e *LRUStore[K, V]) Close() {
	e.closeOnce.Do(func() { e.cancel(); close(e.stop) })
	<-e.done
}

func (e *LRUStore[K, V]) startEviction() {
	defer close(e.done)
	ticker := time.NewTicker(e.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			e.evict(e.getCurrentTime())
		case <-e.stop:
			return
		case <-e.lifetime.Done():
			return
		}
	}
}

func (e *LRUStore[K, V]) Put(key K, value V) bool {
	e.Lock()
	defer e.Unlock()
	return e.putLocked(key, value)
}

// PutContext updates an entry after a cancellable lock wait, returning ctx's
// error if canceled before mutation. It does not start any background work.
func (e *LRUStore[K, V]) PutContext(ctx context.Context, key K, value V) (bool, error) {
	if err := utils.LockContext(ctx, e.TryLock, e.Unlock); err != nil {
		return false, err
	}
	defer e.Unlock()
	return e.putLocked(key, value), nil
}

func (e *LRUStore[K, V]) putLocked(key K, value V) bool {
	if entry, exists := e.freeTable[key]; exists {
		entry.lastAccessTime = e.getCurrentTime()
		entry.Value = value
		e.lruList.moveToHead(entry)
		return false
	}

	entry := &entry[K, V]{Key: key, Value: value, lastAccessTime: e.getCurrentTime()}
	e.lruList.addToHead(entry)
	e.freeTable[key] = entry
	if len(e.freeTable) > e.cap {
		removed := e.lruList.removeTail()
		if removed == nil {
			return false
		}
		delete(e.freeTable, removed.Key)
		return true
	}
	return false
}

func (e *LRUStore[K, V]) Get(key K) (V, bool) {
	e.RLock()
	defer e.RUnlock()
	return e.getLocked(key)
}

// GetContext reads an entry after a cancellable lock wait. Cancellation returns
// the zero value, false, and ctx's error without changing the stored entry.
func (e *LRUStore[K, V]) GetContext(ctx context.Context, key K) (V, bool, error) {
	if err := utils.LockContext(ctx, e.TryRLock, e.RUnlock); err != nil {
		var zero V
		return zero, false, err
	}
	defer e.RUnlock()
	value, exists := e.getLocked(key)
	return value, exists, nil
}

func (e *LRUStore[K, V]) getLocked(key K) (V, bool) {
	if entry, exists := e.freeTable[key]; exists {
		return entry.Value, true
	}
	var zero V
	return zero, false
}

func (e *LRUStore[K, V]) Len() int {
	e.RLock()
	defer e.RUnlock()
	return len(e.freeTable)
}

// Range calls f for each key-value pair in the store; iteration stops if f returns false.
func (e *LRUStore[K, V]) Range(f func(key K, value V) bool) {
	_ = e.RangeContext(context.Background(), f)
}

// RangeContext cancels the lock wait and checks ctx between entries. The
// callback runs under the read lock and must not block or mutate this store.
func (e *LRUStore[K, V]) RangeContext(ctx context.Context, f func(key K, value V) bool) error {
	if err := utils.LockContext(ctx, e.TryRLock, e.RUnlock); err != nil {
		return err
	}
	defer e.RUnlock()
	for k, ent := range e.freeTable {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !f(k, ent.Value) {
			return nil
		}
	}
	return ctx.Err()
}

func (e *LRUStore[K, V]) evict(now time.Time) {
	var keysToEvict []K

	if err := utils.LockContext(e.lifetime, e.TryRLock, e.RUnlock); err != nil {
		return
	}
	for key, entry := range e.freeTable {
		if now.Sub(entry.lastAccessTime) > e.ttl {
			keysToEvict = append(keysToEvict, key)
		}
	}
	e.RUnlock()

	for _, key := range keysToEvict {
		if err := utils.LockContext(e.lifetime, e.TryLock, e.Unlock); err != nil {
			return
		}
		if entry, exists := e.freeTable[key]; exists && now.Sub(entry.lastAccessTime) > e.ttl {
			e.lruList.remove(entry)
			delete(e.freeTable, key)
		}
		e.Unlock()
	}
}

type entry[K comparable, V any] struct {
	Key            K
	Value          V
	prev           *entry[K, V]
	next           *entry[K, V]
	lastAccessTime time.Time
}

type list[K comparable, V any] struct {
	head *entry[K, V]
	tail *entry[K, V]
}

func (l *list[K, V]) addToHead(e *entry[K, V]) {
	e.prev = l.head
	e.next = l.head.next
	l.head.next.prev = e
	l.head.next = e
}

func (l *list[K, V]) moveToHead(e *entry[K, V]) {
	l.remove(e)
	l.addToHead(e)
}

func (l *list[K, V]) remove(e *entry[K, V]) {
	if e.prev != nil {
		e.prev.next = e.next
	}
	if e.next != nil {
		e.next.prev = e.prev
	}
	e.prev = nil
	e.next = nil
}

func (l *list[K, V]) removeTail() *entry[K, V] {
	if l.tail.prev == l.head {
		return nil
	}
	entry := l.tail.prev
	l.remove(entry)
	return entry
}
