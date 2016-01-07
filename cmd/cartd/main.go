package main

import (
	"fmt"
  "flag"
	"log"
	"cart"
	"net/http"
	"os"
	"strconv"

	"github.com/BurntSushi/toml"
)

const (
	// DefaultPort represents the default port the server runs on
  DefaultPort = 8097

	// DefaultBindAddress represents the ip address the server binds to
	DefaultBindAddress = "0.0.0.0"

  // Number of shards to user for locking and storage.
  NShards = 1024

  // Where to store the storage shards.
  ShardDirPath = "shards/"
)


func main() {

  var flush = flag.Bool("flush", false, "Flush the persistent storage.")
  flag.Parse()

  if *flush {
    cart.RemoveContents("shards/")
  }
	// Parse configuration.
	c, err := ParseConfigFile("cmd/cartd/cart.sample.toml")
	if err != nil {
		fmt.Println("Failed to load configuration:", err.Error())
		os.Exit(1)
	}

	// Create handler.
	h := cart.NewHandler()

	// Start HTTP server.
	fmt.Println("Starting cart server on", c.Address())

  // Associate a function with each query type.
  http.HandleFunc("/add", h.Mod(cart.AddToSet))
  http.HandleFunc("/remove", h.Mod(cart.RemoveFromSet))
  http.HandleFunc("/list", h.List)
  http.HandleFunc("/ping", h.Ping)

  // Creates a new service goroutine for each requst.
  log.Fatal(http.ListenAndServe(c.Address(), nil))
}

// Config represents the configuration format.
type Config struct {
	BindAddress string `toml:"bind-address"`
	Port        int    `toml:"port"`
}

// NewConfig returns an instance of Config with default values
func NewConfig() (*Config, error) {
	c := &Config{}
	c.BindAddress = DefaultBindAddress
	c.Port = DefaultPort

	return c, nil
}

// ParseConfigFile parses a configuration file at a given path
func ParseConfigFile(path string) (*Config, error) {
	c, err := NewConfig()
	if err != nil {
		return nil, err
	}
	if _, err := toml.DecodeFile(path, &c); err != nil {
		return nil, err
	}
	return c, nil
}

// Address returns the concatenated IP address and port
func (c *Config) Address() string {
	return c.BindAddress + ":" + strconv.Itoa(c.Port)
}
