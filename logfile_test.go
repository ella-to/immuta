package immuta_test

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"ella.to/immuta"
)

func TestBasicUsage(t *testing.T) {
	t.Parallel()

	namespace := "default"

	storage, celanup := createStorage(t, "./TestBasicUsage", namespace)
	defer celanup()

	content := []byte("hello world")

	index, size, err := func(ctx context.Context) (index int64, size int64, err error) {
		defer storage.Save(namespace, &err)
		return storage.Append(ctx, namespace, bytes.NewReader(content))
	}(t.Context())
	if err != nil {
		t.Fatalf("failed to append content: %v", err)
	}

	if index != 16 {
		t.Fatalf("expected index 8, got %d", index)
	}

	if size != 11 {
		t.Fatalf("expected size 11, got %d", size)
	}

	stream := storage.Stream(t.Context(), namespace, 0)
	defer stream.Done()

	r, size, err := stream.Next(t.Context())
	if err != nil {
		t.Fatalf("failed to get next record: %v", err)
	}
	defer r.Done()

	if size != int64(len(content)) {
		t.Fatalf("expected size %d, got %d", len(content), size)
	}

	var buf bytes.Buffer
	_, err = buf.ReadFrom(r)
	if err != nil {
		t.Fatalf("failed to read from record: %v", err)
	}

	if buf.String() != string(content) {
		t.Fatalf("content mismatch: expected %q, got %q", string(content), buf.String())
	}
}

func TestRollbackOnPreExistingError(t *testing.T) {
	t.Parallel()

	namespace := "rollback"
	storage, cleanup := createStorage(t, "./TestRollbackOnPreExistingError", namespace)
	defer cleanup()

	// Simulate an error before Save is called. The deferred Save should detect
	// a non-nil error and rollback any append that happened.
	func() {
		var err error = errors.New("simulated")
		defer storage.Save(namespace, &err)
		// Append should succeed, but since err is non-nil, Save must truncate.
		if _, _, err := storage.Append(t.Context(), namespace, bytes.NewReader([]byte("temp"))); err != nil {
			t.Fatalf("append failed: %v", err)
		}
	}()

	// Stream should not see any committed content; it should block until a
	// new successful Save is performed. Use a short timeout to ensure blocking.
	stream := storage.Stream(t.Context(), namespace, 0)
	defer stream.Done()

	ctx, cancel := context.WithTimeout(t.Context(), 150*time.Millisecond)
	defer cancel()
	_, _, err := stream.Next(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context deadline exceeded while waiting for content, got: %v", err)
	}

	// Now perform a successful append+save and ensure the content becomes visible.
	if _, _, err := storage.Append(t.Context(), namespace, bytes.NewReader([]byte("final"))); err != nil {
		t.Fatalf("append failed: %v", err)
	}
	var saveErr error
	storage.Save(namespace, &saveErr)
	if saveErr != nil {
		t.Fatalf("save failed: %v", saveErr)
	}

	r, size, err := stream.Next(t.Context())
	if err != nil {
		t.Fatalf("failed to get record after save: %v", err)
	}
	defer r.Done()

	buf := make([]byte, size)
	if _, err := r.Read(buf); err != nil {
		t.Fatalf("failed to read recorded content: %v", err)
	}
	if string(buf) != "final" {
		t.Fatalf("unexpected content: %q", string(buf))
	}
}

func TestStreamWaitsUntilAppendAndSave(t *testing.T) {
	t.Parallel()

	namespace := "wait"
	storage, cleanup := createStorage(t, "./TestStreamWaitsUntilAppendAndSave", namespace)
	defer cleanup()

	stream := storage.Stream(t.Context(), namespace, 0)
	defer stream.Done()

	done := make(chan struct{})
	go func() {
		r, size, err := stream.Next(t.Context())
		if err != nil {
			t.Errorf("stream failed: %v", err)
			close(done)
			return
		}

		buf := make([]byte, size)
		if _, err := r.Read(buf); err != nil {
			t.Errorf("read failed: %v", err)
		}
		if string(buf) != "delayed" {
			t.Errorf("unexpected content: %q", string(buf))
		}
		r.Done()
		close(done)
	}()

	// Give the goroutine a small moment to start and block on Next.
	time.Sleep(50 * time.Millisecond)

	if _, _, err := storage.Append(t.Context(), namespace, bytes.NewReader([]byte("delayed"))); err != nil {
		t.Fatalf("append failed: %v", err)
	}
	var saveErr error
	storage.Save(namespace, &saveErr)
	if saveErr != nil {
		t.Fatalf("save failed: %v", saveErr)
	}

	select {
	case <-done:
		// success
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("timed out waiting for stream to receive message")
	}
}

func TestMultipleAppendsSingleSave(t *testing.T) {
	t.Parallel()

	namespace := "multi"
	storage, cleanup := createStorage(t, "./TestMultipleAppendsSingleSave", namespace)
	defer cleanup()

	msgs := []string{"one", "two", "three"}
	for _, m := range msgs {
		if _, _, err := storage.Append(t.Context(), namespace, bytes.NewReader([]byte(m))); err != nil {
			t.Fatalf("append failed: %v", err)
		}
	}
	var saveErr error
	storage.Save(namespace, &saveErr)
	if saveErr != nil {
		t.Fatalf("save failed: %v", saveErr)
	}

	stream := storage.Stream(t.Context(), namespace, 0)
	defer stream.Done()

	for _, expected := range msgs {
		r, size, err := stream.Next(t.Context())
		if err != nil {
			t.Fatalf("next failed: %v", err)
		}
		buf := make([]byte, size)
		if _, err := r.Read(buf); err != nil {
			t.Fatalf("read failed: %v", err)
		}
		if string(buf) != expected {
			t.Fatalf("expected %q, got %q", expected, string(buf))
		}
		r.Done()
	}
}

func TestMultipleStreams(t *testing.T) {
	t.Parallel()

	namespace := "streams"
	storage, cleanup := createStorage(t, "./TestMultipleStreams", namespace)
	defer cleanup()

	const readers = 3
	streams := make([]immuta.Stream, 0, readers)
	for i := 0; i < readers; i++ {
		streams = append(streams, storage.Stream(t.Context(), namespace, 0))
	}
	for _, s := range streams {
		defer s.Done()
	}

	msgs := []string{"a", "b", "c"}
	for _, m := range msgs {
		if _, _, err := storage.Append(t.Context(), namespace, bytes.NewReader([]byte(m))); err != nil {
			t.Fatalf("append failed: %v", err)
		}
	}
	var saveErr error
	storage.Save(namespace, &saveErr)
	if saveErr != nil {
		t.Fatalf("save failed: %v", saveErr)
	}

	var wg sync.WaitGroup
	wg.Add(readers)
	for i := range readers {
		go func() {
			defer wg.Done()
			for _, expected := range msgs {
				r, size, err := streams[i].Next(t.Context())
				if err != nil {
					t.Errorf("stream %d next failed: %v", i, err)
					return
				}
				buf := make([]byte, size)
				if _, err := r.Read(buf); err != nil {
					t.Errorf("stream %d read failed: %v", i, err)
					return
				}
				if string(buf) != expected {
					t.Errorf("stream %d expected %q got %q", i, expected, string(buf))
					return
				}
				r.Done()
			}
		}()
	}
	wg.Wait()
}
