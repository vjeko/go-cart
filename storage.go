package cart

import (
  "log"
  "fmt"

	"github.com/boltdb/bolt"
)


// The inner storage shard object.
type storageShard struct {
	shardN   uint32  // Shard number/id.
	db      *bolt.DB // A pointer to BoltDB instance.
}

// TODO: Parameterize the storage location in order to
//       facilitate testing
type ShardedStorage struct {
  // A unique type identifier associated with am
  // instance.  This name has to be unique.  It
  // is used for uniquely storing files responsible
  // for each shard.
  name    string
  folder  string
  // An array of storage shard objects.
  shards  [NShards]*storageShard
}

// Given a key, return the storage shard pointer associated
// with it.
func (s *ShardedStorage) getShard(key uint32) *storageShard {
  // Since the key space is bigger, do the module
  // arithmetic to get a proper index.
  idx := key % NShards

  // If there is no shard associated with this index,
  // create a new one (or read the old one).
  if s.shards[idx] == nil {
    //log.Print("shard: creating a new one")
    s.shards[idx] = s.newStorageShard(idx)
  }

  return s.shards[idx]
}


// Given a key, let the function f observe the value 
// associated with it.
func (s *ShardedStorage) ObserveValue(
key uint32, f (func (*setT) error)) error {

  var shard = s.getShard(key)
  var e =  fmt.Errorf("no such key")

  return shard.db.View(func(tx *bolt.Tx) error {
    // Get the bucket.
    bucket := tx.Bucket([]byte("Cart"))
    if bucket == nil {
      return e
    }

    // Get the key byte representation.
    keyBuf, err := getBytes(key)
    if err != nil {
      return err
    }

    // Get the value byte representation.
    data := bucket.Get(keyBuf)
    if data == nil {
      return e
    }

    // Decode the value into its proper type.
    set, err := extractValue(data)
    if err != nil {
      return err
    }

    // Call the observer, passing it the decoded value.
    // No need to check for error.
    f(set)
    return nil
  })
}


// Given a key, let the function f modify the value 
// associated with it.
func (s *ShardedStorage) ChangeValue(key uint32,
value uint32, f (func (*setT, uint32) error)) error {

  var shard = s.getShard(key)

  // From BoltDB documentation:
  // Individual transactions and all objects created from them (e.g.
  // buckets, keys) are not thread safe. To work with data in multiple
  // goroutines you must start a transaction for each one or use
  // locking to ensure only one goroutine accesses a transaction at a
  // time.  Creating transaction from the DB is thread safe.
	return shard.db.Update(func(tx *bolt.Tx) error {
    // Get the bucket, or create a new one if it does not exist.
    bucket, err := tx.CreateBucketIfNotExists([]byte("Cart"))
    if err != nil {
      return err
    }

    // Get the key byte representation.
    keyBuf, err := getBytes(key)
    if err != nil {
      return err
    }

    // Get the key byte representation.
    var data = bucket.Get(keyBuf)

    // Decode the value into its proper type.
    set, err := extractValue(data)
    if err != nil {
      return err
    }

    err = f(set, value)
    if err != nil {
      return err
    }

    // Call the modifier, passing it the decoded value.
		setBuf, err := getBytes(set)

    // Synchronize the change with the block storage.
		err = bucket.Put(keyBuf, setBuf)

		return err
	})
}

// Return a new shard object pointer given the shard id.
func (s *ShardedStorage) newStorageShard(id uint32) *storageShard {
	ss := storageShard{shardN: id, db: NewBoltDB(id, s.name)}
	return &ss
}


// Given a shard id and a shard type-name, either return
// a fresh BoldDB instance or an existing one.
func NewBoltDB(id uint32, name string) *bolt.DB {
  dbPath := fmt.Sprintf("%v/%v-%v.db", ShardDirPath, name, id)
  db, err := bolt.Open(dbPath, 0600, nil)
  if err != nil {
	  log.Fatal(err)
  }
	return db
}

