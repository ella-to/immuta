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

	ctx := t.Context()

	for range n {
		_, _, err := storage.Append(ctx, "default", bytes.NewReader(content))
		if err != nil {
			t.Errorf("failed to append content: %v", err)
		}
	}

	details, err := storage.Details(t.Context(), "default")
	if err != nil {
		t.Fatalf("failed to get details: %v", err)
	}

	fmt.Println("details:", details)

	if err := storage.Verify(t.Context(), "default"); err != nil {
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

		for range n {
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

// TestStreamBlockedByNextUnblocksOnStorageClose tests that when a stream is waiting
// on Next() and storage is closed, the Next() call should unblock and return an error.
func TestStreamBlockedByNextUnblocksOnStorageClose(t *testing.T) {
	storage, cleanup := createStorage(t, "./TestStreamBlockedByNextUnblocksOnStorageClose")
	defer cleanup()

	// Append one message
	_, _, err := storage.Append(context.Background(), "default", strings.NewReader("hello world 0"))
	if err != nil {
		t.Fatalf("failed to append content: %v", err)
	}

	// Create a stream that starts from the latest position (will wait for new messages)
	stream := storage.Stream(context.Background(), "default", -1)
	defer stream.Done()

	// Channel to track when the goroutine starts and completes
	started := make(chan struct{})
	errCh := make(chan error, 1)

	// Start a goroutine that will block on Next()
	go func() {
		close(started) // Signal that we've started

		// This should block because there are no new messages after the latest
		ctx := context.Background()
		r, _, err := stream.Next(ctx)
		if r != nil {
			r.Done()
		}
		errCh <- err
	}()

	// Wait for the goroutine to start
	<-started

	// Give it a moment to actually reach the blocking point
	time.Sleep(100 * time.Millisecond)

	// Close the storage while the goroutine is blocked on Next()
	if err := storage.Close(); err != nil {
		t.Fatalf("failed to close storage: %v", err)
	}

	// Wait for the Next() call to return with a timeout
	select {
	case err := <-errCh:
		// Next() should return an error after storage is closed
		if err == nil {
			t.Fatal("expected Next() to return an error after storage close, got nil")
		}
		// The error should indicate that the storage was closed
		t.Logf("Next() correctly returned error: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("Next() did not unblock within 5 seconds after storage.Close()")
	}
}

// TestMultipleStreamsUnblockOnStorageClose tests that multiple concurrent streams
// all unblock when storage is closed.
func TestMultipleStreamsUnblockOnStorageClose(t *testing.T) {
	storage, cleanup := createStorage(t, "./TestMultipleStreamsUnblockOnStorageClose")
	defer cleanup()

	// Append one message
	_, _, err := storage.Append(context.Background(), "default", bytes.NewReader([]byte("initial message")))
	if err != nil {
		t.Fatalf("failed to append content: %v", err)
	}

	streamCount := 5
	errChs := make([]chan error, streamCount)

	// Create multiple streams that will all block on Next()
	for i := range streamCount {
		errChs[i] = make(chan error, 1)

		stream := storage.Stream(context.Background(), "default", -1)
		defer stream.Done()

		go func(idx int, s immuta.Stream) {
			ctx := context.Background()
			r, _, err := s.Next(ctx)
			if r != nil {
				r.Done()
			}
			errChs[idx] <- err
		}(i, stream)
	}

	// Give all goroutines time to block
	time.Sleep(100 * time.Millisecond)

	// Close the storage
	if err := storage.Close(); err != nil {
		t.Fatalf("failed to close storage: %v", err)
	}

	// All streams should unblock
	for i := range streamCount {
		select {
		case err := <-errChs[i]:
			if err == nil {
				t.Errorf("stream %d: expected error after close, got nil", i)
			} else {
				t.Logf("stream %d: correctly returned error: %v", i, err)
			}
		case <-time.After(5 * time.Second):
			t.Errorf("stream %d: did not unblock within 5 seconds", i)
		}
	}
}

// TestStreamReadThenBlockThenClose tests a stream that successfully reads a message,
// then blocks waiting for the next one, and should unblock when storage closes.
func TestStreamReadThenBlockThenClose(t *testing.T) {
	storage, cleanup := createStorage(t, "./TestStreamReadThenBlockThenClose")
	defer cleanup()

	// Append initial message
	_, _, err := storage.Append(context.Background(), "default", strings.NewReader("message 0"))
	if err != nil {
		t.Fatalf("failed to append content: %v", err)
	}

	// Create stream from beginning
	stream := storage.Stream(context.Background(), "default", 0)
	defer stream.Done()

	// Read the first message successfully
	r, _, err := stream.Next(context.Background())
	if err != nil {
		t.Fatalf("failed to read first message: %v", err)
	}
	r.Done()

	// Now try to read next message (which doesn't exist yet)
	errCh := make(chan error, 1)
	go func() {
		ctx := context.Background()
		r, _, err := stream.Next(ctx)
		if r != nil {
			r.Done()
		}
		errCh <- err
	}()

	// Give it time to block
	time.Sleep(100 * time.Millisecond)

	// Close storage
	if err := storage.Close(); err != nil {
		t.Fatalf("failed to close storage: %v", err)
	}

	// Should unblock with error
	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("expected error after storage close, got nil")
		}
		t.Logf("correctly returned error: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("Next() did not unblock within 5 seconds")
	}
}

// TestStreamWithCancelledContextBeforeClose ensures context cancellation still works
func TestStreamWithCancelledContextBeforeClose(t *testing.T) {
	storage, cleanup := createStorage(t, "./TestStreamWithCancelledContextBeforeClose")
	defer cleanup()

	stream := storage.Stream(context.Background(), "default", 0)
	defer stream.Done()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	r, _, err := stream.Next(ctx)
	if r != nil {
		r.Done()
	}

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context.DeadlineExceeded, got: %v", err)
	}
}

// TestManyStreamsLimitedFDs tests that we can create many more streams than available file descriptors.
// This demonstrates that streams only hold FDs while actively reading, not while waiting.
func TestManyStreamsLimitedFDs(t *testing.T) {
	t.Parallel()

	// Create storage with only 2 reader FDs
	os.RemoveAll("./TestManyStreamsLimitedFDs")
	defer os.RemoveAll("./TestManyStreamsLimitedFDs")

	storage, err := immuta.New(
		immuta.WithFastWrite(true),
		immuta.WithLogsDirPath("./TestManyStreamsLimitedFDs"),
		immuta.WithNamespaces("default"),
		immuta.WithReaderCount(2), // Only 2 FDs available
	)
	if err != nil {
		t.Fatalf("failed to create storage: %v", err)
	}
	defer storage.Close()

	// Write some initial messages
	messagesCount := 10
	for i := 0; i < messagesCount; i++ {
		_, _, err := storage.Append(context.Background(), "default", strings.NewReader(fmt.Sprintf("message %d", i)))
		if err != nil {
			t.Fatalf("failed to append message %d: %v", i, err)
		}
	}

	// Create 20 concurrent streams (10x more than FD count)
	streamsCount := 20
	var wg sync.WaitGroup
	wg.Add(streamsCount)

	errCh := make(chan error, streamsCount)

	for streamIdx := range streamsCount {
		go func(idx int) {
			defer wg.Done()

			stream := storage.Stream(context.Background(), "default", 0)
			defer stream.Done()

			// Each stream reads all messages
			for msgIdx := 0; msgIdx < messagesCount; msgIdx++ {
				r, _, err := stream.Next(context.Background())
				if err != nil {
					errCh <- fmt.Errorf("stream %d failed to read message %d: %w", idx, msgIdx, err)
					return
				}

				// Actually read the data
				buf := new(bytes.Buffer)
				if _, err := buf.ReadFrom(r); err != nil {
					r.Done()
					errCh <- fmt.Errorf("stream %d failed to read content %d: %w", idx, msgIdx, err)
					return
				}
				r.Done()

				expected := fmt.Sprintf("message %d", msgIdx)
				if buf.String() != expected {
					errCh <- fmt.Errorf("stream %d message %d: expected %q, got %q", idx, msgIdx, expected, buf.String())
					return
				}
			}
		}(streamIdx)
	}

	wg.Wait()
	close(errCh)

	// Check for errors
	for err := range errCh {
		t.Error(err)
	}
}

// TestStreamsWaitingForDataShareFDs tests that multiple streams can read concurrently
// with limited FD pool. Streams naturally serialize when competing for FDs.
func TestStreamsWaitingForDataShareFDs(t *testing.T) {
	t.Parallel()

	os.RemoveAll("./TestStreamsWaitingForDataShareFDs")
	defer os.RemoveAll("./TestStreamsWaitingForDataShareFDs")

	storage, err := immuta.New(
		immuta.WithFastWrite(true),
		immuta.WithLogsDirPath("./TestStreamsWaitingForDataShareFDs"),
		immuta.WithNamespaces("default"),
		immuta.WithReaderCount(3), // Only 3 FDs
	)
	if err != nil {
		t.Fatalf("failed to create storage: %v", err)
	}
	defer storage.Close()

	// Write initial messages
	messagesCount := 10
	for i := 0; i < messagesCount; i++ {
		_, _, err := storage.Append(context.Background(), "default", strings.NewReader(fmt.Sprintf("data %d", i)))
		if err != nil {
			t.Fatalf("failed to append message %d: %v", i, err)
		}
	}

	// Create 10 concurrent streams (more than available FDs)
	// With limited FDs, reads will naturally serialize
	streamsCount := 10
	var wg sync.WaitGroup
	wg.Add(streamsCount)

	errCh := make(chan error, streamsCount)

	// Start concurrent readers - no artificial timeout, let them compete for FDs naturally
	for streamIdx := 0; streamIdx < streamsCount; streamIdx++ {
		go func(idx int) {
			defer wg.Done()

			stream := storage.Stream(context.Background(), "default", 0)
			defer stream.Done()

			for msgIdx := 0; msgIdx < messagesCount; msgIdx++ {
				// Use background context - let FD acquisition block naturally
				r, _, err := stream.Next(context.Background())
				if err != nil {
					errCh <- fmt.Errorf("stream %d failed at message %d: %w", idx, msgIdx, err)
					return
				}

				buf := new(bytes.Buffer)
				if _, err := buf.ReadFrom(r); err != nil {
					r.Done()
					errCh <- fmt.Errorf("stream %d failed to read content %d: %w", idx, msgIdx, err)
					return
				}
				r.Done()

				expected := fmt.Sprintf("data %d", msgIdx)
				if buf.String() != expected {
					errCh <- fmt.Errorf("stream %d message %d: expected %q, got %q", idx, msgIdx, expected, buf.String())
					return
				}
			}
		}(streamIdx)
	}

	// Wait with timeout for the whole test
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Success
	case <-time.After(30 * time.Second):
		t.Fatal("test timed out - possible deadlock")
	}

	close(errCh)

	// Check for errors
	for err := range errCh {
		t.Error(err)
	}
}

// TestFDReleaseOnStreamDone verifies that FDs are not leaked when streams are done
func TestFDReleaseOnStreamDone(t *testing.T) {
	t.Parallel()

	os.RemoveAll("./TestFDReleaseOnStreamDone")
	defer os.RemoveAll("./TestFDReleaseOnStreamDone")

	storage, err := immuta.New(
		immuta.WithFastWrite(true),
		immuta.WithLogsDirPath("./TestFDReleaseOnStreamDone"),
		immuta.WithNamespaces("default"),
		immuta.WithReaderCount(2),
	)
	if err != nil {
		t.Fatalf("failed to create storage: %v", err)
	}
	defer storage.Close()

	// Write a message
	_, _, err = storage.Append(context.Background(), "default", strings.NewReader("test"))
	if err != nil {
		t.Fatalf("failed to append: %v", err)
	}

	// Create and immediately close many streams
	for i := 0; i < 100; i++ {
		stream := storage.Stream(context.Background(), "default", 0)
		r, _, err := stream.Next(context.Background())
		if err != nil {
			t.Fatalf("iteration %d: failed to read: %v", i, err)
		}
		io.Copy(io.Discard, r)
		r.Done()
		stream.Done()
	}

	// If FDs were leaked, this would fail or deadlock
	stream := storage.Stream(context.Background(), "default", 0)
	defer stream.Done()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	r, _, err := stream.Next(ctx)
	if err != nil {
		t.Fatalf("failed to read after many stream cycles: %v", err)
	}
	defer r.Done()

	buf := new(bytes.Buffer)
	if _, err := buf.ReadFrom(r); err != nil {
		t.Fatalf("failed to read content: %v", err)
	}

	if buf.String() != "test" {
		t.Fatalf("expected 'test', got %q", buf.String())
	}
}

// BenchmarkManyStreamsLimitedFDs benchmarks concurrent stream reads with limited FDs.
// This demonstrates the efficiency of FD sharing across many streams.
func BenchmarkManyStreamsLimitedFDs(b *testing.B) {
	os.RemoveAll("./BenchmarkManyStreamsLimitedFDs")
	defer os.RemoveAll("./BenchmarkManyStreamsLimitedFDs")

	storage, err := immuta.New(
		immuta.WithFastWrite(true),
		immuta.WithLogsDirPath("./BenchmarkManyStreamsLimitedFDs"),
		immuta.WithNamespaces("default"),
		immuta.WithReaderCount(5), // Limited FDs
	)
	if err != nil {
		b.Fatalf("failed to create storage: %v", err)
	}
	defer storage.Close()

	// Pre-populate with messages
	messagesCount := 100
	content := []byte(strings.Repeat("x", 1024)) // 1KB messages
	for i := 0; i < messagesCount; i++ {
		_, _, err := storage.Append(context.Background(), "default", bytes.NewReader(content))
		if err != nil {
			b.Fatalf("failed to append: %v", err)
		}
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Create 20 concurrent streams (4x FD count)
		streamsCount := 20
		var wg sync.WaitGroup
		wg.Add(streamsCount)

		for j := 0; j < streamsCount; j++ {
			go func() {
				defer wg.Done()

				stream := storage.Stream(context.Background(), "default", 0)
				defer stream.Done()

				for k := 0; k < messagesCount; k++ {
					r, _, err := stream.Next(context.Background())
					if err != nil {
						b.Errorf("failed to read: %v", err)
						return
					}
					io.Copy(io.Discard, r)
					r.Done()
				}
			}()
		}

		wg.Wait()
	}
}

// BenchmarkFDContention benchmarks worst-case FD contention scenario
func BenchmarkFDContention(b *testing.B) {
	os.RemoveAll("./BenchmarkFDContention")
	defer os.RemoveAll("./BenchmarkFDContention")

	storage, err := immuta.New(
		immuta.WithFastWrite(true),
		immuta.WithLogsDirPath("./BenchmarkFDContention"),
		immuta.WithNamespaces("default"),
		immuta.WithReaderCount(2), // Very limited FDs
	)
	if err != nil {
		b.Fatalf("failed to create storage: %v", err)
	}
	defer storage.Close()

	// Pre-populate
	for i := 0; i < 10; i++ {
		_, _, err := storage.Append(context.Background(), "default", strings.NewReader(fmt.Sprintf("msg %d", i)))
		if err != nil {
			b.Fatalf("failed to append: %v", err)
		}
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// 50 concurrent streams competing for 2 FDs
		streamsCount := 50
		var wg sync.WaitGroup
		wg.Add(streamsCount)

		for j := 0; j < streamsCount; j++ {
			go func() {
				defer wg.Done()

				stream := storage.Stream(context.Background(), "default", 0)
				defer stream.Done()

				for k := 0; k < 10; k++ {
					r, _, err := stream.Next(context.Background())
					if err != nil {
						b.Errorf("failed to read: %v", err)
						return
					}
					io.Copy(io.Discard, r)
					r.Done()
				}
			}()
		}

		wg.Wait()
	}
}
