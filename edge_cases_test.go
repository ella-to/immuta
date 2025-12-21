package immuta_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"ella.to/immuta"
)

// Edge case tests for immuta storage

// TestEmptyMessage tests appending and reading an empty message
func TestEmptyMessage(t *testing.T) {
	t.Parallel()

	storage, cleanup := createStorage(t, "./TestEmptyMessage")
	defer cleanup()

	// Append empty content
	index, size, err := storage.Append(context.Background(), "default", bytes.NewReader([]byte{}))
	if err != nil {
		t.Fatalf("failed to append empty content: %v", err)
	}

	if size != 0 {
		t.Fatalf("expected size 0, got %d", size)
	}

	if index != immuta.FileHeaderSize {
		t.Fatalf("expected index %d, got %d", immuta.FileHeaderSize, index)
	}

	// Read it back
	stream := storage.Stream(context.Background(), "default", 0)
	defer stream.Done()

	r, readSize, err := stream.Next(context.Background())
	if err != nil {
		t.Fatalf("failed to read empty content: %v", err)
	}
	defer r.Done()

	if readSize != 0 {
		t.Fatalf("expected read size 0, got %d", readSize)
	}

	buf := new(bytes.Buffer)
	_, err = buf.ReadFrom(r)
	if err != nil {
		t.Fatalf("failed to read content: %v", err)
	}

	if buf.Len() != 0 {
		t.Fatalf("expected empty buffer, got %d bytes", buf.Len())
	}
}

// TestLargeMessage tests appending and reading a large message (1MB)
func TestLargeMessage(t *testing.T) {
	t.Parallel()

	storage, cleanup := createStorage(t, "./TestLargeMessage")
	defer cleanup()

	// Create 1MB content
	content := bytes.Repeat([]byte("x"), 1024*1024)

	_, size, err := storage.Append(context.Background(), "default", bytes.NewReader(content))
	if err != nil {
		t.Fatalf("failed to append large content: %v", err)
	}

	if size != int64(len(content)) {
		t.Fatalf("expected size %d, got %d", len(content), size)
	}

	// Read it back
	stream := storage.Stream(context.Background(), "default", 0)
	defer stream.Done()

	r, _, err := stream.Next(context.Background())
	if err != nil {
		t.Fatalf("failed to read large content: %v", err)
	}
	defer r.Done()

	buf := new(bytes.Buffer)
	_, err = buf.ReadFrom(r)
	if err != nil {
		t.Fatalf("failed to read content: %v", err)
	}

	if !bytes.Equal(buf.Bytes(), content) {
		t.Fatal("content mismatch for large message")
	}
}

// TestAppendAfterClose tests that append fails after storage is closed
func TestAppendAfterClose(t *testing.T) {
	t.Parallel()

	storage, _ := createStorage(t, "./TestAppendAfterClose")
	defer os.RemoveAll("./TestAppendAfterClose")

	// Close storage
	if err := storage.Close(); err != nil {
		t.Fatalf("failed to close storage: %v", err)
	}

	// Try to append after close
	_, _, err := storage.Append(context.Background(), "default", strings.NewReader("test"))
	if !errors.Is(err, immuta.ErrStorageClosed) {
		t.Fatalf("expected ErrStorageClosed, got %v", err)
	}
}

// TestDoubleClose tests that closing storage twice doesn't panic
func TestDoubleClose(t *testing.T) {
	t.Parallel()

	storage, _ := createStorage(t, "./TestDoubleClose")
	defer os.RemoveAll("./TestDoubleClose")

	// First close
	if err := storage.Close(); err != nil {
		t.Fatalf("first close failed: %v", err)
	}

	// Second close should not panic and return nil
	if err := storage.Close(); err != nil {
		t.Fatalf("second close should return nil, got %v", err)
	}
}

// TestStreamDoneMultipleTimes tests that calling Done multiple times doesn't panic
func TestStreamDoneMultipleTimes(t *testing.T) {
	t.Parallel()

	storage, cleanup := createStorage(t, "./TestStreamDoneMultipleTimes")
	defer cleanup()

	stream := storage.Stream(context.Background(), "default", 0)

	// Call Done multiple times - should not panic
	stream.Done()
	stream.Done()
	stream.Done()
}

// TestContextCancellation tests that context cancellation is properly handled
func TestContextCancellation(t *testing.T) {
	t.Parallel()

	storage, cleanup := createStorage(t, "./TestContextCancellation")
	defer cleanup()

	stream := storage.Stream(context.Background(), "default", 0)
	defer stream.Done()

	// Create a context that's already cancelled
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, _, err := stream.Next(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

// TestConcurrentAppendAndStream tests concurrent appending and streaming
func TestConcurrentAppendAndStream(t *testing.T) {
	t.Parallel()

	storage, cleanup := createStorage(t, "./TestConcurrentAppendAndStream")
	defer cleanup()

	numMessages := 1000
	numReaders := 5

	var wg sync.WaitGroup
	var appendErr atomic.Value
	var readErrors atomic.Int64

	// Start writer
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < numMessages; i++ {
			_, _, err := storage.Append(context.Background(), "default", strings.NewReader("message"))
			if err != nil {
				appendErr.Store(err)
				return
			}
		}
	}()

	// Start readers
	for i := 0; i < numReaders; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			stream := storage.Stream(context.Background(), "default", 0)
			defer stream.Done()

			for j := 0; j < numMessages/numReaders; j++ {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				r, _, err := stream.Next(ctx)
				cancel()
				if err != nil {
					if !errors.Is(err, context.DeadlineExceeded) {
						readErrors.Add(1)
					}
					return
				}
				io.Copy(io.Discard, r)
				r.Done()
			}
		}()
	}

	wg.Wait()

	if err, ok := appendErr.Load().(error); ok && err != nil {
		t.Fatalf("append error: %v", err)
	}

	if readErrors.Load() > 0 {
		t.Fatalf("had %d read errors", readErrors.Load())
	}
}

// TestStreamFromDifferentPositions tests streaming from various start positions
func TestStreamFromDifferentPositions(t *testing.T) {
	t.Parallel()

	storage, cleanup := createStorage(t, "./TestStreamFromDifferentPositions")
	defer cleanup()

	// Append 10 messages
	messages := make([]string, 10)
	for i := 0; i < 10; i++ {
		messages[i] = strings.Repeat("x", i+1)
		_, _, err := storage.Append(context.Background(), "default", strings.NewReader(messages[i]))
		if err != nil {
			t.Fatalf("failed to append: %v", err)
		}
	}

	testCases := []struct {
		name          string
		startPos      int64
		expectedFirst string
	}{
		{"from beginning", 0, messages[0]},
		{"skip 5", 5, messages[5]},
		{"skip 9", 9, messages[9]},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			stream := storage.Stream(context.Background(), "default", tc.startPos)
			defer stream.Done()

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			r, _, err := stream.Next(ctx)
			if err != nil {
				t.Fatalf("failed to read: %v", err)
			}
			defer r.Done()

			buf := new(bytes.Buffer)
			buf.ReadFrom(r)

			if buf.String() != tc.expectedFirst {
				t.Fatalf("expected %q, got %q", tc.expectedFirst, buf.String())
			}
		})
	}
}

// TestStreamFromLatest tests streaming from the latest position (-1)
func TestStreamFromLatest(t *testing.T) {
	t.Parallel()

	storage, cleanup := createStorage(t, "./TestStreamFromLatest")
	defer cleanup()

	// Append some initial messages
	for i := 0; i < 5; i++ {
		_, _, err := storage.Append(context.Background(), "default", strings.NewReader("old message"))
		if err != nil {
			t.Fatalf("failed to append: %v", err)
		}
	}

	// Create stream from latest
	stream := storage.Stream(context.Background(), "default", -1)
	defer stream.Done()

	// Append new message
	expectedMsg := "new message after stream creation"
	go func() {
		time.Sleep(50 * time.Millisecond)
		storage.Append(context.Background(), "default", strings.NewReader(expectedMsg))
	}()

	// Should receive the new message, not old ones
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	r, _, err := stream.Next(ctx)
	if err != nil {
		t.Fatalf("failed to read: %v", err)
	}
	defer r.Done()

	buf := new(bytes.Buffer)
	buf.ReadFrom(r)

	if buf.String() != expectedMsg {
		t.Fatalf("expected %q, got %q", expectedMsg, buf.String())
	}
}

// TestInvalidNamespace tests accessing invalid namespaces
func TestInvalidNamespace(t *testing.T) {
	t.Parallel()

	storage, cleanup := createStorage(t, "./TestInvalidNamespace")
	defer cleanup()

	// Test empty namespace
	_, _, err := storage.Append(context.Background(), "", strings.NewReader("test"))
	if !errors.Is(err, immuta.ErrNamespaceRequired) {
		t.Fatalf("expected ErrNamespaceRequired, got %v", err)
	}

	// Test non-existent namespace
	_, _, err = storage.Append(context.Background(), "nonexistent", strings.NewReader("test"))
	if !errors.Is(err, immuta.ErrNamesapceNotFound) {
		t.Fatalf("expected ErrNamesapceNotFound, got %v", err)
	}
}

// TestVerifyAfterAppend tests that Verify succeeds after appending
func TestVerifyAfterAppend(t *testing.T) {
	t.Parallel()

	ctx := t.Context()

	storage, cleanup := createStorage(t, "./TestVerifyAfterAppend")
	defer cleanup()

	for i := 0; i < 100; i++ {
		_, _, err := storage.Append(context.Background(), "default", strings.NewReader(strings.Repeat("x", i+1)))
		if err != nil {
			t.Fatalf("failed to append: %v", err)
		}
	}

	if err := storage.Verify(ctx, "default"); err != nil {
		t.Fatalf("verification failed: %v", err)
	}
}

// TestReaderDoneMultipleTimes tests that calling Reader.Done multiple times doesn't cause issues
func TestReaderDoneMultipleTimes(t *testing.T) {
	t.Parallel()

	storage, cleanup := createStorage(t, "./TestReaderDoneMultipleTimes")
	defer cleanup()

	_, _, err := storage.Append(context.Background(), "default", strings.NewReader("test"))
	if err != nil {
		t.Fatalf("failed to append: %v", err)
	}

	stream := storage.Stream(context.Background(), "default", 0)
	defer stream.Done()

	r, _, err := stream.Next(context.Background())
	if err != nil {
		t.Fatalf("failed to read: %v", err)
	}

	// Read the content first
	io.Copy(io.Discard, r)

	// Call Done multiple times
	r.Done()
	// Second call might return error but shouldn't panic
	r.Done()
}

// TestStorageReopenAfterClose tests that storage can be reopened after close
func TestStorageReopenAfterClose(t *testing.T) {
	path := "./TestStorageReopenAfterClose"
	os.RemoveAll(path)
	defer os.RemoveAll(path)

	// Create and write some data
	storage1, err := immuta.New(
		immuta.WithFastWrite(true),
		immuta.WithLogsDirPath(path),
		immuta.WithNamespaces("default"),
	)
	if err != nil {
		t.Fatalf("failed to create storage: %v", err)
	}

	messages := []string{"message1", "message2", "message3"}
	for _, msg := range messages {
		_, _, err := storage1.Append(context.Background(), "default", strings.NewReader(msg))
		if err != nil {
			t.Fatalf("failed to append: %v", err)
		}
	}

	storage1.Close()

	// Reopen and verify data
	storage2, err := immuta.New(
		immuta.WithFastWrite(true),
		immuta.WithLogsDirPath(path),
		immuta.WithNamespaces("default"),
	)
	if err != nil {
		t.Fatalf("failed to reopen storage: %v", err)
	}
	defer storage2.Close()

	stream := storage2.Stream(context.Background(), "default", 0)
	defer stream.Done()

	for i, expected := range messages {
		r, _, err := stream.Next(context.Background())
		if err != nil {
			t.Fatalf("failed to read message %d: %v", i, err)
		}

		buf := new(bytes.Buffer)
		buf.ReadFrom(r)
		r.Done()

		if buf.String() != expected {
			t.Fatalf("message %d: expected %q, got %q", i, expected, buf.String())
		}
	}

	// Append more data after reopen
	_, _, err = storage2.Append(context.Background(), "default", strings.NewReader("message4"))
	if err != nil {
		t.Fatalf("failed to append after reopen: %v", err)
	}

	if err := storage2.Verify(t.Context(), "default"); err != nil {
		t.Fatalf("verification failed after reopen: %v", err)
	}
}

// TestMultipleNamespaces tests using multiple namespaces
func TestMultipleNamespaces(t *testing.T) {
	t.Parallel()

	path := "./TestMultipleNamespaces"
	os.RemoveAll(path)
	defer os.RemoveAll(path)

	storage, err := immuta.New(
		immuta.WithFastWrite(true),
		immuta.WithLogsDirPath(path),
		immuta.WithNamespaces("ns1", "ns2", "ns3"),
	)
	if err != nil {
		t.Fatalf("failed to create storage: %v", err)
	}
	defer storage.Close()

	// Write different data to each namespace
	namespaces := []string{"ns1", "ns2", "ns3"}
	for i, ns := range namespaces {
		for j := 0; j < 10; j++ {
			msg := strings.Repeat(string(rune('a'+i)), j+1)
			_, _, err := storage.Append(context.Background(), ns, strings.NewReader(msg))
			if err != nil {
				t.Fatalf("failed to append to %s: %v", ns, err)
			}
		}
	}

	// Verify each namespace
	for _, ns := range namespaces {
		if err := storage.Verify(t.Context(), ns); err != nil {
			t.Fatalf("verification failed for %s: %v", ns, err)
		}
	}
}

// TestSkipMoreThanAvailable tests skipping more messages than available
func TestSkipMoreThanAvailable(t *testing.T) {
	t.Parallel()

	storage, cleanup := createStorage(t, "./TestSkipMoreThanAvailable")
	defer cleanup()

	// Append only 3 messages
	for i := 0; i < 3; i++ {
		_, _, err := storage.Append(context.Background(), "default", strings.NewReader("msg"))
		if err != nil {
			t.Fatalf("failed to append: %v", err)
		}
	}

	// Create stream trying to skip 100 messages
	stream := storage.Stream(context.Background(), "default", 100)
	defer stream.Done()

	// Should wait for new messages since we skipped past all existing ones
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_, _, err := stream.Next(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected timeout when skipping past all messages, got %v", err)
	}
}

// TestDetailsAfterClose tests that Details returns error after close
func TestDetailsAfterClose(t *testing.T) {
	t.Parallel()

	storage, _ := createStorage(t, "./TestDetailsAfterClose")
	defer os.RemoveAll("./TestDetailsAfterClose")

	storage.Close()

	_, err := storage.Details(t.Context(), "default")
	if !errors.Is(err, immuta.ErrStorageClosed) {
		t.Fatalf("expected ErrStorageClosed, got %v", err)
	}
}

// TestVerifyAfterClose tests that Verify returns error after close
func TestVerifyAfterClose(t *testing.T) {
	t.Parallel()

	storage, _ := createStorage(t, "./TestVerifyAfterClose")
	defer os.RemoveAll("./TestVerifyAfterClose")

	storage.Close()

	err := storage.Verify(t.Context(), "default")
	if !errors.Is(err, immuta.ErrStorageClosed) {
		t.Fatalf("expected ErrStorageClosed, got %v", err)
	}
}
