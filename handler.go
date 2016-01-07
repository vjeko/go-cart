package cart

import (
	"fmt"
  "strconv"
	"net/http"
)
// Handler represents the HTTP handler for the customer API.
type Handler struct {
	http.Handler
  cLock ShardedLock
  iLock ShardedLock
  cStorage ShardedStorage
  iStorage ShardedStorage
}

// NewHandler returns a new instance of Handler.
func NewHandler() *Handler {
  h := Handler{
    cStorage: ShardedStorage{name: "customer"},
    iStorage: ShardedStorage{name: "item"},
  }
	return &h
}

func (h* Handler) Ping(w http.ResponseWriter, r *http.Request) {
  fmt.Fprintf(w, "ping\n")
}

// This function is responsible for handling /list queries.
// A valid parameter for the list query is either an item or a
// customer id, but not both.
func (h* Handler) List(w http.ResponseWriter, r *http.Request) {

  // A helper function for communication with the client.
  var printcustomer = func (w http.ResponseWriter) (func(*setT) error) {
    return func(s *setT) error {
      fmt.Fprintf(w, "OK\n",)
      for k, v := range *s {
        fmt.Fprintf(w, "%v %v\n", k, v)
      }
      return nil
    }
  }

  // Make sure that only one parameter is being passed to this
  // handler.  Otherwise, report an error to the client.
  if len(r.URL.Query()) != 1 {
    err := fmt.Errorf("you can specify only one arg")
    fmt.Fprintf(w, "error: %v", err)
    return
  }

  // Try to get both an item and a customer id.
  item, itemErr := h.checkItemArg(w, r)
  customer, customerErr := h.checkCustomerArg(w, r)

  // Make sure at least one of them is set.
  if ((customerErr == nil) == (itemErr == nil)) {
    fmt.Fprintf(w, "error: %v: %v", itemErr, customerErr)
    return
  }

  // Depending on the parameter type, use the proper
  // storage and locking instances.
  var key uint32
  var storage *ShardedStorage
  var lock *ShardedLock

  // List customer ids associated with an item?
  if itemErr == nil {
    key = uint32(item)
    storage = &h.iStorage
    lock = &h.iLock
  }

  // List items associated with a customer id.
  if customerErr == nil {
    key = uint32(customer)
    storage = &h.cStorage
    lock = &h.cLock
  }

  // Try to acquire the lock.
  if !lock.TryLock(key) {
    // We failed. Let the client know.
    w.WriteHeader(http.StatusServiceUnavailable)
    return
  }
  // in case we succeeded, make sure to release the lock
  // when we're done.
  defer lock.MustUnlock(key)

  // Ask the underlying storage for the corresponding
  // value.  The result is going to be handled by
  // printcustomer(w) function.
  err := storage.ObserveValue(key, printcustomer(w))
  if (err != nil) {
    fmt.Fprintf(w, "error: %v", err)
    return
  }

}

// This function is responsible for handling /add and /remove
// queries.  Two necessary parameters are the customer id and 
// the item.
func (h* Handler) Mod(f (func (*setT, uint32) error)) func(w http.ResponseWriter, r *http.Request) {
  return func(w http.ResponseWriter, r *http.Request) {

    // Make sure that only two parameters are being passed to 
    // this handler.  Otherwise, report an error to the client.
    if len(r.URL.Query()) != 2 {
      err := fmt.Errorf("you need to specity two args")
      fmt.Fprintf(w, "error: %v", err)
      return
    }

    // Make sure we have the customer id parameter.
    customer, err := h.checkCustomerArg(w, r)
    if err != nil {
      fmt.Fprintf(w, "error: %v", err)
    }

    // Make sure we have the item parameter.
    item, err := h.checkItemArg(w, r)
    if err != nil {
      fmt.Fprintf(w, "error: %v", err)
    }

    // We need to acquire both locks.  One for the item shard
    // and the other one for the customer id shard
    if !h.cLock.TryLock(uint32(customer)) {
      w.WriteHeader(http.StatusServiceUnavailable)
      return
    }
    defer h.cLock.MustUnlock(uint32(customer))

    if !h.iLock.TryLock(uint32(item)) {
      w.WriteHeader(http.StatusServiceUnavailable)
      return
    }
    defer h.iLock.MustUnlock(uint32(item))

    // Update the customer's customer by making appropriate changes.
    err = h.cStorage.ChangeValue(uint32(customer), uint32(item), f)
    if (err != nil) {
      fmt.Fprintf(w, "error: %v", err)
      return
    }

    // Update the mapping between an item and customers that have it
    // in their customers.
    err = h.iStorage.ChangeValue(uint32(item), uint32(customer), f)
    if (err != nil) {
      fmt.Fprintf(w, "error: %v", err)
      return
    }

    // TODO: In case the later action fails, we need to revert
    // the first change.

    fmt.Fprintf(w, "OK\n")
  }
}


// Verify that the item parameter is passed properly.
func (h* Handler) checkItemArg(
    w http.ResponseWriter, r *http.Request) (itemID, error) {

  // Is it even there?
  itemStr, ok := r.URL.Query()["item"]
  if (!ok) {
    err := fmt.Errorf("item missing")
    return 0, err
  }

  // There is only one value?
  if len(itemStr) > 1 {
    err := fmt.Errorf("only one item allowed")
    return 0, err
  }

  // is it a proper int?
  // TODO: Check boundaries.
  item, err := strconv.Atoi(itemStr[0])
  if (err != nil) {
    err := fmt.Errorf("invalid item id %v", item)
    return 0, err
  }

  return itemID(item), nil
}


// An optional cleanup function.
func (h* Handler) Close() {
  for _, storage := range [...]ShardedStorage{h.cStorage, h.iStorage} {
    for _, s := range storage.shards {
      if s == nil {
        continue
      }

      if s.db == nil {
        continue
      }

      s.db.Close()
    }
  }
}

// Verify that the customer id parameter is passed properly.
func (h* Handler) checkCustomerArg(
    w http.ResponseWriter, r *http.Request) (customerID, error) {

  // Is it even there?
  customerStr, ok := r.URL.Query()["customer"]
  if (!ok) {
    err := fmt.Errorf("customer missing")
    return 0, err
  }

  // There is only one value?
  if len(customerStr) > 1 {
    err := fmt.Errorf("only one customer allowed")
    return 0, err
  }

  // is it a proper int?
  // TODO: Check boundaries.
  customer, err := strconv.Atoi(customerStr[0])
  if (err != nil) {
    err := fmt.Errorf("invalid customer id %v", customerStr)
    return 0, err
  }

  return customerID(customer), nil
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/ping":
		fmt.Fprintf(w, "ping\n")
	default:
		http.NotFound(w, r)
	}
}

