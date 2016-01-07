package cart

import (
  "log"
  "sync/atomic"
)

// A non-recursive, non-blocking lock implementation.
// Multiple keys might share the same lock depending
// on what the number of shards is.
type ShardedLock struct {
  shards [NShards]uint32
}

// Given a key, try to acquire the lock.
// Return success or failure.
func (l* ShardedLock) TryLock(key uint32) bool {
  idx := key % NShards
  loc := &l.shards[idx]
  swapped := atomic.CompareAndSwapUint32(loc, 0, 1)
  return swapped
}

// Given a key, release the lock.
// The invocation must/should never fail.
func (l* ShardedLock) MustUnlock(key uint32) bool {
  idx := key % NShards
  loc := &l.shards[idx]
  swapped := atomic.CompareAndSwapUint32(loc, 1, 0)
  if !swapped {
    println("Should never happen")
    log.Fatal("internal error: releasing the lock failed")
  }
  return swapped
}
