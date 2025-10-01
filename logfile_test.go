package immuta_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"ella.to/immuta"
)

func createStorage(t testing.TB, path string) (*immuta.Storage, func()) {
	os.RemoveAll(path)

	storage, err := immuta.New(
		immuta.WithFastWrite(true),
		immuta.WithLogsDirPath(path),
		immuta.WithNamespaces("default"),
	)
	if err != nil {
		t.Fatalf("failed to create storage: %v", err)
	}
	return storage, func() {
		storage.Close()
		os.RemoveAll(path)
	}
}

func TestBasicUsage(t *testing.T) {
	t.Parallel()

	storage, celanup := createStorage(t, "./TestBasicUsage")
	defer celanup()

	content := []byte("hello world")

	index, size, err := storage.Append(context.Background(), "default", bytes.NewReader(content))
	if err != nil {
		t.Fatalf("failed to append content: %v", err)
	}

	if index != immuta.FileHeaderSize {
		t.Fatalf("expected index to be 8, got %d", index)
	}

	if size != int64(len(content)) {
		t.Fatalf("expected size to be %d, got %d", len(content), size)
	}

	stream := storage.Stream(context.Background(), "default", 0)
	if err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}
	defer stream.Done()

	r, size, err := stream.Next(context.Background())
	if err != nil {
		t.Fatalf("failed to read content: %v", err)
	}
	defer r.Done()

	if size != int64(len(content)) {
		t.Fatalf("expected size to be %d, got %d", len(content), size)
	}

	buf := new(bytes.Buffer)
	if _, err := buf.ReadFrom(r); err != nil {
		t.Fatalf("failed to read content: %v", err)
	}

	if !bytes.Equal(buf.Bytes(), content) {
		t.Fatalf("expected content to be %v, got %v", content, buf.Bytes())
	}
}

func TestSingleWriteSingleReader(t *testing.T) {
	t.Parallel()

	messagesCount := 2

	storage, cleanup := createStorage(t, "./TestSingleWriteSingleReader")
	defer cleanup()

	var wg sync.WaitGroup

	wg.Add(2)

	go func() {
		defer wg.Done()

		for i := range messagesCount {
			_, _, err := storage.Append(context.Background(), "default", strings.NewReader(fmt.Sprintf("hello world %d", i)))
			if err != nil {
				t.Errorf("failed to append content: %v", err)
			}
		}
	}()

	go func() {
		defer wg.Done()

		stream := storage.Stream(context.Background(), "default", 0)
		defer stream.Done()

		for i := range messagesCount {
			func() {
				expectedContent := fmt.Sprintf("hello world %d", i)

				r, size, err := stream.Next(context.Background())
				if err != nil {
					t.Errorf("failed to read content: %v", err)
				}
				defer r.Done()

				buf := new(bytes.Buffer)
				if _, err := buf.ReadFrom(r); err != nil {
					t.Errorf("failed to read content: %v", err)
				}

				if size != int64(len(expectedContent)) {
					t.Errorf("expected size to be %d, got %d", len(expectedContent), size)
				}

				if buf.String() != expectedContent {
					t.Errorf("expected content to be %v, got %v", expectedContent, buf.String())
				}
			}()
		}
	}()

	wg.Wait()
}

func TestSkipNMessages(t *testing.T) {
	t.Parallel()

	storage, cleanup := createStorage(t, "./TestSkipNMessages")
	defer cleanup()

	content := []byte("a")

	n := 10

	for i := 0; i < n; i++ {
		_, _, err := storage.Append(context.Background(), "default", bytes.NewReader(content))
		if err != nil {
			t.Fatalf("failed to append content: %v", err)
		}
	}

	stream := storage.Stream(context.Background(), "default", 5)

	count := 0

	for {
		ok := func() bool {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			r, size, err := stream.Next(ctx)
			if errors.Is(err, context.DeadlineExceeded) {
				return false
			} else if err != nil {
				t.Errorf("failed to read content 1: %v", err)
				return false
			}
			defer r.Done()

			buf := new(bytes.Buffer)
			if _, err := buf.ReadFrom(r); err != nil {
				t.Errorf("failed to read content 2: %v", err)
				return false
			}

			if size != int64(len(content)) {
				t.Errorf("expected size to be %d, got %d", len(content), size)
				return false
			}
			return true
		}()
		if !ok {
			break
		}

		count++
	}

	if count != 5 {
		t.Fatalf("expected to read 5 times, got %d", count)
	}
}

func TestDetails(t *testing.T) {
	storage, cleanup := createStorage(t, "./TestDetails")
	defer cleanup()

	content := []byte("hello world")

	n := 10

	for range n {
		_, _, err := storage.Append(context.Background(), "default", bytes.NewReader(content))
		if err != nil {
			t.Errorf("failed to append content: %v", err)
		}
	}

	details, err := storage.Details("default")
	if err != nil {
		t.Fatalf("failed to get details: %v", err)
	}

	fmt.Println("details:", details)

	if err := storage.Verify("default"); err != nil {
		t.Fatalf("failed to verify storage: %v", err)
	}
}

func TestSingleWriteMultipleReader(t *testing.T) {
	t.Parallel()

	storage, cleanup := createStorage(t, "./TestSingleWriteMultipleReader")
	defer cleanup()

	content := []byte("hello world")

	n := 100_000
	readersCount := 11

	var wg sync.WaitGroup

	wg.Add(readersCount + 1)

	go func() {
		defer wg.Done()

		for i := 0; i < n; i++ {
			_, _, err := storage.Append(context.Background(), "default", bytes.NewReader(content))
			if err != nil {
				t.Errorf("failed to append content: %v", err)
			}
		}
	}()

	for range readersCount {
		go func() {
			defer wg.Done()

			stream := storage.Stream(context.Background(), "default", 0)

			count := 0
			for {
				ok := func() bool {
					ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
					defer cancel()

					r, size, err := stream.Next(ctx)
					if errors.Is(err, context.DeadlineExceeded) {
						return false
					} else if err != nil {
						t.Errorf("failed to read content: %v", err)
						return false
					}
					defer r.Done()

					buf := new(bytes.Buffer)
					if _, err := buf.ReadFrom(r); err != nil {
						t.Errorf("failed to read content: %v", err)
						return false
					}

					if size != int64(len(content)) {
						t.Errorf("expected size to be %d, got %d: %s", len(content), size, buf.String())
						return false
					}
					return true
				}()
				if !ok {
					break
				}

				count++
			}

			if count != n {
				t.Errorf("expected to read 10 times, got %d", count)
			}
		}()
	}

	wg.Wait()
}

func Benchmark1kbAppend(b *testing.B) {
	storage, cleanup := createStorage(b, "./Benchmark1kbAppend")
	defer cleanup()

	content := []byte(strings.Repeat("a", 1024))

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _, err := storage.Append(context.Background(), "default", bytes.NewReader(content))
		if err != nil {
			b.Fatalf("failed to append content: %v", err)
		}
	}
}

func Benchmark4kbAppend(b *testing.B) {
	storage, cleanup := createStorage(b, "./Benchmark1kbAppend")
	defer cleanup()

	content := []byte(strings.Repeat("a", 4*1024))

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _, err := storage.Append(context.Background(), "default", bytes.NewReader(content))
		if err != nil {
			b.Fatalf("failed to append content: %v", err)
		}
	}
}

func TestRead100kMessages(t *testing.T) {
	t.Parallel()

	storage, cleanup := createStorage(t, "./TestRead10Messages")
	defer cleanup()

	content := []byte("hello world")

	n := 100_000

	start := time.Now()

	for i := 0; i < n; i++ {
		_, _, err := storage.Append(context.Background(), "default", bytes.NewReader(content))
		if err != nil {
			t.Fatalf("failed to append content: %v", err)
		}
	}

	fmt.Printf("time taken to write %d: %v\n", n, time.Since(start))

	stream := storage.Stream(context.Background(), "default", 0)

	start = time.Now()
	defer func() {
		fmt.Printf("time taken to read %d: %v\n", n, time.Since(start))
	}()

	for i := 0; i < n; i++ {
		func() {
			r, _, err := stream.Next(context.Background())
			if err != nil {
				t.Fatalf("failed to read content: %v", err)
			}
			defer r.Done()

			io.Copy(io.Discard, r)
		}()
	}
}

func TestSequence(t *testing.T) {
	t.Parallel()

	storage, cleanup := createStorage(t, "./TestSequence")
	defer cleanup()

	n := 10
	c := 2

	var wg sync.WaitGroup

	wg.Add(c + 1)

	go func() {
		defer wg.Done()
		for i := range n {
			r := strings.NewReader(fmt.Sprintf("hello world %d", i))
			_, _, err := storage.Append(context.Background(), "default", r)
			if err != nil {
				t.Errorf("failed to append content: %v", err)
				return
			}
		}
	}()

	for range c {
		go func() {
			defer wg.Done()
			stream := storage.Stream(context.Background(), "default", 0)
			defer stream.Done()

			for i := range n {
				func() {
					expectedContent := fmt.Sprintf("hello world %d", i)

					r, size, err := stream.Next(context.Background())
					if err != nil {
						t.Errorf("failed to read content: %v", err)
						return
					}
					defer r.Done()

					buf := new(bytes.Buffer)
					if _, err := buf.ReadFrom(r); err != nil {
						t.Errorf("failed to read content: %v", err)
						return
					}

					if size != int64(len(expectedContent)) {
						t.Errorf("expected size to be %d, got %d", len(expectedContent), size)
						return
					}

					if buf.String() != fmt.Sprintf("hello world %d", i) {
						t.Errorf("expected content to be %v, got %v", expectedContent, buf.String())
						return
					}
				}()
			}
		}()
	}

	wg.Wait()
}

func TestJson(t *testing.T) {
	t.Parallel()

	storage, cleanup := createStorage(t, "./TestJson")
	defer cleanup()

	_, _, err := storage.Append(context.Background(), "default", strings.NewReader(`{"id":0,"name":"hello world 0"}`))
	if err != nil {
		t.Fatalf("failed to append content: %v", err)
	}

	stream := storage.Stream(context.Background(), "default", 0)
	defer stream.Done()

	r, _, err := stream.Next(context.Background())
	if err != nil {
		t.Fatalf("failed to read content: %v", err)
	}
	defer r.Done()

	var m map[string]interface{}
	err = json.NewDecoder(r).Decode(&m)
	if err != nil {
		t.Fatalf("failed to decode json: %v", err)
	}

	if m == nil {
		t.Fatalf("expected map to be non-nil")
	}
}

func TestCreateStopRead(t *testing.T) {
	os.RemoveAll("./TestCreateStopRead")
	defer os.RemoveAll("./TestCreateStopRead")

	storage, err := immuta.New(
		immuta.WithFastWrite(true),
		immuta.WithLogsDirPath("./TestCreateStopRead"),
		immuta.WithNamespaces("default"),
	)
	if err != nil {
		t.Fatalf("failed to create storage: %v", err)
	}

	for i := range 10 {
		_, _, appendErr := storage.Append(context.Background(), "default", strings.NewReader(fmt.Sprintf("hello world %d", i)))
		if appendErr != nil {
			t.Fatalf("failed to append content: %v", err)
		}
	}

	storage.Close()

	storage, err = immuta.New(
		immuta.WithFastWrite(true),
		immuta.WithLogsDirPath("./TestCreateStopRead"),
		immuta.WithNamespaces("default"),
	)
	if err != nil {
		t.Fatalf("failed to create storage: %v", err)
	}

	stream := storage.Stream(context.Background(), "default", 0)
	defer stream.Done()

	for i := range 10 {
		func() {
			r, _, err := stream.Next(context.Background())
			if err != nil {
				t.Errorf("failed to read content: %v", err)
			}
			defer r.Done()

			buf := new(bytes.Buffer)
			if _, err := buf.ReadFrom(r); err != nil {
				t.Errorf("failed to read content: %v", err)
			}

			if buf.String() != fmt.Sprintf("hello world %d", i) {
				t.Errorf("expected content to be %s, got %s", fmt.Sprintf("hello world %d", i), buf.String())
			}
		}()
	}
}

func TestStartLatest(t *testing.T) {
	t.Parallel()

	storage, cleanup := createStorage(t, "./TestStartLatest")
	defer cleanup()

	_, _, err := storage.Append(context.Background(), "default", strings.NewReader("hello world 0"))
	if err != nil {
		t.Fatalf("failed to append content: %v", err)
	}

	ch := make(chan struct{})

	go func() {
		<-ch
		index, size, appendErr := storage.Append(context.Background(), "default", strings.NewReader("hello world 1"))
		if appendErr != nil {
			t.Errorf("failed to append content: %v", err)
		}

		fmt.Println("index:", index, "size:", size)
	}()

	stream := storage.Stream(context.Background(), "default", -1)
	defer stream.Done()

	close(ch)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Second)
	defer cancel()

	r, _, err := stream.Next(ctx)
	if err != nil {
		t.Fatalf("failed to read content: %v", err)
	}
	defer r.Done()

	buf := new(bytes.Buffer)
	if _, err := buf.ReadFrom(r); err != nil {
		t.Fatalf("failed to read content: %v", err)
	}

	if buf.String() != "hello world 1" {
		t.Fatalf("expected content to be %s, got %s", "hello world 1", buf.String())
	}
}
