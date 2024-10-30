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

func createStorage(t testing.TB, filename string) *immuta.Storage {
	storage, err := immuta.New(filename, 10, true)
	t.Cleanup(func() {
		storage.Close()
		os.Remove(filename)
	})
	if err != nil {
		t.Fatalf("failed to create storage: %v", err)
	}
	return storage
}

func TestBasicUsage(t *testing.T) {
	t.Parallel()

	storage := createStorage(t, "./TestBasicUsage.log")

	content := []byte("hello world")

	size, err := storage.Append(context.Background(), bytes.NewReader(content))
	if err != nil {
		t.Fatalf("failed to append content: %v", err)
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
	t.Parallel()

	storage := createStorage(t, "./TestSingleWriteSingleReader.log")

	content := []byte("hello world")

	go func() {
		for i := 0; i < 10; i++ {
			_, err := storage.Append(context.Background(), bytes.NewReader(content))
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
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
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
	t.Parallel()

	storage := createStorage(t, "./TestSkipNMessages.log")

	content := []byte("a")

	n := 10

	for i := 0; i < n; i++ {
		_, err := storage.Append(context.Background(), bytes.NewReader(content))
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

func TestSingleWriteMultipleReader(t *testing.T) {
	t.Parallel()

	storage := createStorage(t, "./TestSingleWriteSingleReader.log")

	content := []byte("hello world")

	n := 100_000

	go func() {
		for i := 0; i < n; i++ {
			_, err := storage.Append(context.Background(), bytes.NewReader(content))
			if err != nil {
				t.Errorf("failed to append content: %v", err)
			}
		}
	}()

	stream1, err := storage.Stream(context.Background(), 0)
	if err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}

	stream2, err := storage.Stream(context.Background(), 0)
	if err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}

	var wg sync.WaitGroup

	wg.Add(2)

	go func() {
		defer wg.Done()

		count := 0
		for {
			ok := func() bool {
				ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
				defer cancel()

				r, size, err := stream1.Next(ctx)
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

		if count != n {
			t.Errorf("expected to read 10 times, got %d", count)
		}
	}()

	go func() {
		defer wg.Done()

		count := 0
		for {
			ok := func() bool {
				ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
				defer cancel()

				r, size, err := stream2.Next(ctx)
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

		if count != n {
			t.Errorf("expected to read 10 times, got %d", count)
		}
	}()

	wg.Wait()
}

func Benchmark1kbAppend(b *testing.B) {
	storage := createStorage(b, "./Benchmark1kbAppend.log")

	content := []byte(strings.Repeat("a", 1024))

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := storage.Append(context.Background(), bytes.NewReader(content))
		if err != nil {
			b.Fatalf("failed to append content: %v", err)
		}
	}
}

func Benchmark4kbAppend(b *testing.B) {
	storage := createStorage(b, "./Benchmark1kbAppend.log")

	content := []byte(strings.Repeat("a", 4*1024))

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := storage.Append(context.Background(), bytes.NewReader(content))
		if err != nil {
			b.Fatalf("failed to append content: %v", err)
		}
	}
}

func TestRead100kMessages(t *testing.T) {
	t.Parallel()

	storage := createStorage(t, "./TestRead10Messages.log")

	content := []byte("hello world")

	n := 100_000

	start := time.Now()

	for i := 0; i < n; i++ {
		_, err := storage.Append(context.Background(), bytes.NewReader(content))
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
