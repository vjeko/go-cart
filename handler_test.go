package cart_test

import (
  "fmt"
	"net/http"
	"net/http/httptest"
	"testing"
  "strings"
  "math/rand"
	"sync"
  "cart"
	"strconv"
//	"time"
)

const (
	NumberOfIterations = 100
	NumberOfThreads = 10
	NumberOfThreadIterations = 100
)

// Ensure the Handler can respond to a ping request.
func TestHandler_Ping(t *testing.T) {
	r, err := http.NewRequest("GET", "http://localhost/ping", nil)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	w := httptest.NewRecorder()
	cart.NewHandler().Ping(w, r)

	if w.Body.String() != "ping\n" {
		t.Fatalf("expected `ping`, got `%s`", w.Body.String())
	} else if w.Code != 200 {
		t.Fatalf("expected status code 200, got %d", w.Code)
	}
}

func TestHandler_Add(t *testing.T) {

  var item uint32 = 1234
  var customer uint32 = 4321

  r := modRequest(t, "add", customer, item)
	w := httptest.NewRecorder()
  h := cart.NewHandler()
  h.Mod(cart.AddToSet)(w, r)

  h.Close()
  cart.RemoveContents("shards/")

	if w.Body.String() != "OK\n" {
		t.Fatalf("expected `OK`, got `%s`", w.Body.String())
	} else if w.Code != 200 {
		t.Fatalf("expected status code 200, got %d", w.Code)
	}
}

func TestHandler_Remove(t *testing.T) {

  var item uint32 = 1234
  var customer uint32 = 4321

  r := modRequest(t, "remove", customer, item)
	w := httptest.NewRecorder()
  h := cart.NewHandler()
  h.Mod(cart.RemoveFromSet)(w, r)

  h.Close()
  cart.RemoveContents("shards/")

  if !strings.HasPrefix(w.Body.String(), "error:") {
    t.Fatalf("expected `error:`, got `%s`", w.Body.String())
  }
}

func listRequest(
  t *testing.T, op string, key uint32) *http.Request {

  request := fmt.Sprintf(
    "http://localhost/list?%v=%v", op, key)
	r, err := http.NewRequest("GET", request, nil)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

  return r
}



func modRequest(
  t *testing.T, op string,
  customer uint32, item uint32) *http.Request {

  request := fmt.Sprintf(
    "http://localhost/%v?customer=%v&item=%v", op, customer, item)
	r, err := http.NewRequest("GET", request, nil)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

  return r
}


type tuple struct {
 customer uint32
 item uint32
}


func parallelAdd(t *testing.T, h* cart.Handler,
wg* sync.WaitGroup,
data chan map[uint32]map[uint32]uint32) {

	basket := make(map[uint32]map[uint32]uint32)
  w := httptest.NewRecorder()
	var slice []tuple

  for i := 0; i <= NumberOfThreadIterations; i++ {
    var item uint32 = uint32(rand.Intn(200))
    var customer uint32 = uint32(rand.Intn(100))

		i := tuple{customer, item}
		slice = append(slice, i)
	}

	for _, pair  := range slice {
    r := modRequest(t, "add", pair.customer, pair.item)
retry:
    h.Mod(cart.AddToSet)(w, r)

		if w.Code == 503 {
			goto retry
		}

		if basket[pair.customer] == nil {
			basket[pair.customer] = make(map[uint32]uint32)
		}

		basket[pair.customer][pair.item]++
    if !strings.HasPrefix(w.Body.String(), "OK") {
			t.Fatalf("expected `OK`, got `%s`", w.Body.String())
    }
  }

	data <- basket
	wg.Done()
}

func aggregator(data chan map[uint32]map[uint32]uint32,
							  done chan map[uint32]map[uint32]uint32) {
	total := make(map[uint32]map[uint32]uint32)
	counter := 0
	for {
		select {
			case m := <-data:
				counter++
				for k1, v1 := range m {
					for k2, v2 := range v1 {
						if (total[k1] == nil) {
							total[k1] = make(map[uint32]uint32)
						}
						total[k1][k2] += v2
					}
				}
				if counter == NumberOfThreads {
					done <- total
				}
		}
	}
}


func checkCorrectness(
t *testing.T, data map[uint32]map[uint32]uint32,h *cart.Handler) {

	for customer, _ := range data {
		r := listRequest(t, "customer", customer)
		w := httptest.NewRecorder()
		h.List(w, r)
		lines := strings.Split(w.Body.String(),"\n")
		for i := 1; i < len(lines); i++ {
			p := strings.Split(lines[i]," ")
			if len(p) < 2 {
				continue
			}
			i, err := strconv.Atoi(p[0])
			if err != nil {
				t.Fatalf("unexpected error: %s", err)
			}

			n, err := strconv.Atoi(p[1])
			if err != nil {
				t.Fatalf("unexpected error: %s", err)
			}

			if (data[customer][uint32(i)] != uint32(n)) {
				t.Fatalf("customer %v has %v of %v instead of %v",
					customer, n)
			}
		}
	}
}

func TestHandler_ParallelAdd(t *testing.T) {
  h := cart.NewHandler()
	var wg sync.WaitGroup

	data := make(chan map[uint32]map[uint32]uint32)
	done := make(chan map[uint32]map[uint32]uint32)

	go aggregator(data, done)

  for i := 0; i < NumberOfThreads; i++ {
		wg.Add(1)
		go parallelAdd(t, h, &wg, data)
	}

	total := <-done
  h.Close()
	checkCorrectness(t, total, h)
  h.Close()
  cart.RemoveContents("shards/")
}



func TestHandler_SequentialAdd(t *testing.T) {
  w := httptest.NewRecorder()
  h := cart.NewHandler()

	var slice []tuple

  for i := 0; i <= NumberOfIterations; i++ {
    var item uint32 = uint32(rand.Intn(2048))
    var customer uint32 = uint32(rand.Intn(2048))

		i := tuple{customer, item}
		slice = append(slice, i)
	}

	for _, pair  := range slice {
    r := modRequest(t, "add", pair.customer, pair.item)
    h.Mod(cart.AddToSet)(w, r)

    if !strings.HasPrefix(w.Body.String(), "OK") {
      t.Fatalf("expected `OK`, got `%s`", w.Body.String())
    }
  }

  h.Close()
  cart.RemoveContents("shards/")
}


func TestHandler_SequentialAddRemove(t *testing.T) {
  w := httptest.NewRecorder()
  h := cart.NewHandler()

	var slice []tuple

  for i := 0; i <= NumberOfIterations; i++ {
    var item uint32 = uint32(rand.Intn(2048))
    var customer uint32 = uint32(rand.Intn(2048))

		i := tuple{customer, item}
		slice = append(slice, i)
	}

	for _, pair  := range slice {
    r := modRequest(t, "add", pair.customer, pair.item)
    h.Mod(cart.AddToSet)(w, r)

    if !strings.HasPrefix(w.Body.String(), "OK") {
      t.Fatalf("expected `OK`, got `%s`", w.Body.String())
    }
  }

	for _, pair  := range slice {
    r := modRequest(t, "remove", pair.customer, pair.item)
    h.Mod(cart.RemoveFromSet)(w, r)

    if !strings.HasPrefix(w.Body.String(), "OK") {
      t.Fatalf("expected `OK`, got `%s`", w.Body.String())
    }
  }

  h.Close()
  cart.RemoveContents("shards/")
}





// Ensure the Handler returns a 404 Not Found for an unknown path.
func TestHandler_NotFound(t *testing.T) {
	r, err := http.NewRequest("GET", "http://localhost/foo", nil)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	w := httptest.NewRecorder()
	cart.NewHandler().ServeHTTP(w, r)

	if w.Code != 404 {
		t.Fatalf("expected status code 404, got %d", w.Code)
	}
}
