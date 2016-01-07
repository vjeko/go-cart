package cart

import (
  "fmt"
  "os"
  "path/filepath"

  "bytes"
  "encoding/gob"
)


// Given a generic key, return a binary representation of
// it using the gob encoder.
func getBytes(key interface{}) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(key)
	if err != nil {
			return nil, err
	}
	return buf.Bytes(), nil
}

// Given a binary representation of an object, return its
// decoded value using the gob decoder.
func extractValue(data []byte) (*setT, error) {
  var set setT

  if data == nil {
    //log.Print("no set associated with that key")
    set = make(setT)
    return &set, nil
  }

  buf := bytes.NewBuffer(data)
  dec := gob.NewDecoder(buf)
  err := dec.Decode(&set)
  if err != nil {
    return nil, err
  }

  return &set, nil
}

// A helper function that adds an item to an existing set.
// If item key is already there, it increments the count,
// otherwise it initializes it to one..
func AddToSet(s *setT, value uint32) error {
  if _, ok := (*s)[value]; ok {
    (*s)[value] = (*s)[value] + 1
    return nil
  }

  (*s)[value] = 1
  return nil
}

// A helper function that removes an item from an existing set.
// If the item is already there, it decrements the count,
// otherwise it removes the item altogether.
func RemoveFromSet(s *setT, value uint32) error {
  size, ok := (*s)[value]
  if !ok {
    return fmt.Errorf("item not in the cart")
  }

  if size == 1 {
    delete(*s, value)
    return nil
  }

  (*s)[value] = (*s)[value] - 1
  return nil
}

// Remove all the files in the specified directory.
func RemoveContents(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
			return err
	}
	defer d.Close()

	names, err := d.Readdirnames(-1)
	if err != nil {
			return err
	}

	for _, name := range names {
			err = os.RemoveAll(filepath.Join(dir, name))
			if err != nil {
					return err
			}
	}
	return nil
}

