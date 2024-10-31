package immuta_test

import (
	"bytes"
	"context"
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

func createStorage(t testing.TB, filename string) (*immuta.Storage, func()) {
	os.Remove(filename)

	storage, err := immuta.New(filename, 10, true)
	if err != nil {
		t.Fatalf("failed to create storage: %v", err)
	}
	return storage, func() {
		storage.Close()
		os.Remove(filename)
	}
}

func TestBasicUsage(t *testing.T) {
	// t.Parallel()

	storage, celanup := createStorage(t, "./TestBasicUsage.log")
	defer celanup()

	content := []byte("hello world")

	index, size, err := storage.Append(context.Background(), bytes.NewReader(content))
	if err != nil {
		t.Fatalf("failed to append content: %v", err)
	}

	if index != 8 {
		t.Fatalf("expected index to be 8, got %d", index)
	}

	if size != int64(len(content)) {
		t.Fatalf("expected size to be %d, got %d", len(content), size)
	}

	stream, err := storage.Stream(context.Background(), 0)
	if err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}
	defer stream.Done()

	r, size, err := stream.Next(context.Background())
	if err != nil {
		t.Fatalf("failed to read content: %v", err)
	}

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
	// t.Parallel()

	storage, cleanup := createStorage(t, "./TestSingleWriteSingleReader.log")
	defer cleanup()

	content := []byte("hello world")

	go func() {
		for i := 0; i < 10; i++ {
			_, _, err := storage.Append(context.Background(), bytes.NewReader(content))
			if err != nil {
				t.Errorf("failed to append content: %v", err)
			}
		}
	}()

	stream, err := storage.Stream(context.Background(), 0)
	if err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}

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

			buf := new(bytes.Buffer)
			if _, err := buf.ReadFrom(r); err != nil {
				t.Errorf("failed to read content: %v", err)
				return false
			}

			if buf.String() != "hello world" {
				t.Errorf("expected content to be %s, got %s", "hello world", buf.String())
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

	if count != 10 {
		t.Fatalf("expected to read 10 times, got %d", count)
	}
}

func TestSkipNMessages(t *testing.T) {
	// t.Parallel()

	storage, cleanup := createStorage(t, "./TestSkipNMessages.log")
	defer cleanup()

	content := []byte("a")

	n := 10

	for i := 0; i < n; i++ {
		_, _, err := storage.Append(context.Background(), bytes.NewReader(content))
		if err != nil {
			t.Fatalf("failed to append content: %v", err)
		}
	}

	stream, err := storage.Stream(context.Background(), 5)
	if err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}

	count := 0

	for {
		ok := func() bool {
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			defer cancel()

			r, size, err := stream.Next(ctx)
			if errors.Is(err, context.DeadlineExceeded) {
				return false
			} else if err != nil {
				t.Fatalf("failed to read content: %v", err)
				return false
			}

			buf := new(bytes.Buffer)
			if _, err := buf.ReadFrom(r); err != nil {
				t.Fatalf("failed to read content: %v", err)
				return false
			}

			if size != int64(len(content)) {
				t.Fatalf("expected size to be %d, got %d", len(content), size)
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
	storage, cleanup := createStorage(t, "./TestDetails.log")
	defer cleanup()

	content := []byte("hello world")

	n := 10

	for i := 0; i < n; i++ {
		_, _, err := storage.Append(context.Background(), bytes.NewReader(content))
		if err != nil {
			t.Errorf("failed to append content: %v", err)
		}
	}

	details, err := storage.Details()
	if err != nil {
		t.Fatalf("failed to get details: %v", err)
	}

	fmt.Println("details:", details)

	if err := storage.Verify(); err != nil {
		t.Fatalf("failed to verify storage: %v", err)
	}
}

func TestSingleWriteMultipleReader(t *testing.T) {
	// t.Parallel()

	storage, cleanup := createStorage(t, "./TestSingleWriteMultipleReader.log")
	defer cleanup()

	content := []byte("hello world")

	n := 10
	readersCount := 2

	var wg sync.WaitGroup

	wg.Add(readersCount)

	for i := 0; i < n; i++ {
		_, _, err := storage.Append(context.Background(), bytes.NewReader(content))
		if err != nil {
			t.Errorf("failed to append content: %v", err)
		}
	}

	details, err := storage.Details()
	if err != nil {
		t.Fatalf("failed to get details: %v", err)
	}

	fmt.Println("details:", details)

	if err := storage.Verify(); err != nil {
		t.Fatalf("failed to verify storage: %v", err)
	}

	for range readersCount {
		go func() {
			defer wg.Done()

			stream, err := storage.Stream(context.Background(), 0)
			if err != nil {
				t.Errorf("failed to create stream: %v", err)
			}

			fmt.Println("stream created:", stream)

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
	storage, cleanup := createStorage(b, "./Benchmark1kbAppend.log")
	defer cleanup()

	content := []byte(strings.Repeat("a", 1024))

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _, err := storage.Append(context.Background(), bytes.NewReader(content))
		if err != nil {
			b.Fatalf("failed to append content: %v", err)
		}
	}
}

func Benchmark4kbAppend(b *testing.B) {
	storage, cleanup := createStorage(b, "./Benchmark1kbAppend.log")
	defer cleanup()

	content := []byte(strings.Repeat("a", 4*1024))

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _, err := storage.Append(context.Background(), bytes.NewReader(content))
		if err != nil {
			b.Fatalf("failed to append content: %v", err)
		}
	}
}

func TestRead100kMessages(t *testing.T) {
	// t.Parallel()

	storage, cleanup := createStorage(t, "./TestRead10Messages.log")
	defer cleanup()

	content := []byte("hello world")

	n := 100_000

	start := time.Now()

	for i := 0; i < n; i++ {
		_, _, err := storage.Append(context.Background(), bytes.NewReader(content))
		if err != nil {
			t.Fatalf("failed to append content: %v", err)
		}
	}

	fmt.Printf("time taken to write %d: %v\n", n, time.Since(start))

	stream, err := storage.Stream(context.Background(), 0)
	if err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}

	start = time.Now()
	defer func() {
		fmt.Printf("time taken to read %d: %v\n", n, time.Since(start))
	}()

	for i := 0; i < n; i++ {
		r, _, err := stream.Next(context.Background())
		if err != nil {
			t.Fatalf("failed to read content: %v", err)
		}

		io.Copy(io.Discard, r)

		// buf := new(bytes.Buffer)
		// if _, err := buf.ReadFrom(r); err != nil {
		// 	t.Fatalf("failed to read content: %v", err)
		// }

		// if size != int64(len(content)) {
		// 	t.Fatalf("expected size to be %d, got %d", len(content), size)
		// }

		// if !bytes.Equal(buf.Bytes(), content) {
		// 	t.Fatalf("expected content to be %v, got %v", content, buf.Bytes())
		// }
	}
}
