package cart

const (
  // Number of shards to user for locking and storage.
  NShards = 1024

  // Where to store the storage shards.
  ShardDirPath = "shards/"
)

type customerID uint32
type itemID uint32

type setT map[uint32]uint32
