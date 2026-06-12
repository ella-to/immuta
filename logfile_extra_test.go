package immuta_test

import (
	"bytes"
	"compress/flate"
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"ella.to/immuta"
)

// hideLen wraps a reader so the Len() fast path cannot be used, forcing the
// unknown-size (placeholder header + patch) append path.
type hideLen struct{ r io.Reader }

func (h hideLen) Read(p []byte) (int, error) { return h.r.Read(p) }

// failAfter yields n bytes and then returns an error, simulating a source
// that dies in the middle of an append.
type failAfter struct{ n int }

func (f *failAfter) Read(p []byte) (int, error) {
	if f.n <= 0 {
		return 0, errors.New("simulated source failure")
	}
	if len(p) > f.n {
		p = p[:f.n]
	}
	for i := range p {
		p[i] = 'x'
	}
	f.n -= len(p)
	return len(p), nil
}

func appendAndSave(t testing.TB, storage *immuta.Storage, namespace string, content []byte) {
	t.Helper()
	_, _, err := storage.Append(t.Context(), namespace, bytes.NewReader(content))
	storage.Save(namespace, &err)
	if err != nil {
		t.Fatalf("append+save failed: %v", err)
	}
}

func readNext(t testing.TB, stream immuta.Stream) []byte {
	t.Helper()
	r, size, err := stream.Next(t.Context())
	if err != nil {
		t.Fatalf("next failed: %v", err)
	}
	defer r.Done()
	buf := make([]byte, size)
	if _, err := io.ReadFull(r, buf); err != nil {
		t.Fatalf("read failed: %v", err)
	}
	return buf
}

func TestAppendUnknownSizeReader(t *testing.T) {
	t.Parallel()

	namespace := "unknown"
	storage, cleanup := createStorage(t, "./TestAppendUnknownSizeReader", namespace)
	defer cleanup()

	content := []byte("payload of unknown length")
	_, size, err := storage.Append(t.Context(), namespace, hideLen{bytes.NewReader(content)})
	storage.Save(namespace, &err)
	if err != nil {
		t.Fatalf("append failed: %v", err)
	}
	if size != int64(len(content)) {
		t.Fatalf("expected size %d, got %d", len(content), size)
	}

	stream := storage.Stream(t.Context(), namespace, 0)
	defer stream.Done()

	if got := readNext(t, stream); !bytes.Equal(got, content) {
		t.Fatalf("content mismatch: got %q", got)
	}

	if err := storage.Verify(t.Context(), namespace); err != nil {
		t.Fatalf("verify failed: %v", err)
	}
}

func TestAppendLargePayload(t *testing.T) {
	t.Parallel()

	namespace := "large"
	storage, cleanup := createStorage(t, "./TestAppendLargePayload", namespace)
	defer cleanup()

	// larger than the single-write staging threshold (64KB)
	content := bytes.Repeat([]byte("large-payload-"), 10*1024) // ~140KB
	appendAndSave(t, storage, namespace, content)

	// and one through the unknown-size path as well
	_, _, err := storage.Append(t.Context(), namespace, hideLen{bytes.NewReader(content)})
	storage.Save(namespace, &err)
	if err != nil {
		t.Fatalf("append failed: %v", err)
	}

	stream := storage.Stream(t.Context(), namespace, 0)
	defer stream.Done()

	for i := 0; i < 2; i++ {
		if got := readNext(t, stream); !bytes.Equal(got, content) {
			t.Fatalf("record %d content mismatch (len %d vs %d)", i, len(got), len(content))
		}
	}

	if err := storage.Verify(t.Context(), namespace); err != nil {
		t.Fatalf("verify failed: %v", err)
	}
}

func TestAppendFailedReaderLeavesFileConsistent(t *testing.T) {
	t.Parallel()

	namespace := "failmid"
	storage, cleanup := createStorage(t, "./TestAppendFailedReaderLeavesFileConsistent", namespace)
	defer cleanup()

	// a committed record before the failure
	appendAndSave(t, storage, namespace, []byte("before"))

	// the source fails after a few KB; Append must discard the partial record
	// even though Save is never told about it (txPending was false).
	if _, _, err := storage.Append(t.Context(), namespace, hideLen{&failAfter{n: 4096}}); err == nil {
		t.Fatal("expected append to fail")
	}

	// the log must still be fully consistent and writable
	if err := storage.Verify(t.Context(), namespace); err != nil {
		t.Fatalf("verify failed after failed append: %v", err)
	}

	appendAndSave(t, storage, namespace, []byte("after"))

	stream := storage.Stream(t.Context(), namespace, 0)
	defer stream.Done()

	if got := readNext(t, stream); string(got) != "before" {
		t.Fatalf("expected %q, got %q", "before", got)
	}
	if got := readNext(t, stream); string(got) != "after" {
		t.Fatalf("expected %q, got %q", "after", got)
	}

	if err := storage.Verify(t.Context(), namespace); err != nil {
		t.Fatalf("verify failed: %v", err)
	}
}

func TestStreamStartPosMiddle(t *testing.T) {
	t.Parallel()

	namespace := "seek"
	storage, cleanup := createStorage(t, "./TestStreamStartPosMiddle", namespace)
	defer cleanup()

	const total = 1000
	for i := 0; i < total; i++ {
		// varying sizes so a wrong offset computation cannot accidentally line up
		content := bytes.Repeat([]byte{byte('a' + i%26)}, 1+i%97)
		appendAndSave(t, storage, namespace, content)
	}

	const startPos = 513
	stream := storage.Stream(t.Context(), namespace, startPos)
	defer stream.Done()

	for i := startPos; i < total; i++ {
		expected := bytes.Repeat([]byte{byte('a' + i%26)}, 1+i%97)
		if got := readNext(t, stream); !bytes.Equal(got, expected) {
			t.Fatalf("record %d mismatch: got %d bytes %q...", i, len(got), got[:min(len(got), 8)])
		}
	}
}

func TestStreamStartPosWithRecordsLargerThanScanChunk(t *testing.T) {
	t.Parallel()

	namespace := "bigseek"
	storage, cleanup := createStorage(t, "./TestStreamStartPosWithRecordsLargerThanScanChunk", namespace)
	defer cleanup()

	// records larger than the 512KB header-scan window force the scanner to
	// jump and refill its cache
	big := bytes.Repeat([]byte{'b'}, 600*1024)
	for i := 0; i < 3; i++ {
		appendAndSave(t, storage, namespace, big)
	}
	appendAndSave(t, storage, namespace, []byte("tail"))

	stream := storage.Stream(t.Context(), namespace, 3)
	defer stream.Done()

	if got := readNext(t, stream); string(got) != "tail" {
		t.Fatalf("expected %q, got %d bytes", "tail", len(got))
	}
}

func TestCloseWithActiveStreams(t *testing.T) {
	t.Parallel()

	namespace := "close"
	storage, cleanup := createStorage(t, "./TestCloseWithActiveStreams", namespace)
	defer cleanup()

	appendAndSave(t, storage, namespace, []byte("data"))

	const readers = 8
	var wg sync.WaitGroup
	wg.Add(readers)
	for i := 0; i < readers; i++ {
		go func() {
			defer wg.Done()
			stream := storage.Stream(t.Context(), namespace, 0)
			defer stream.Done()
			for {
				r, _, err := stream.Next(t.Context())
				if err != nil {
					if !errors.Is(err, immuta.ErrStorageClosed) {
						t.Errorf("unexpected error: %v", err)
					}
					return
				}
				io.Copy(io.Discard, r)
				r.Done()
			}
		}()
	}

	// let the readers consume the record and block waiting for more
	time.Sleep(50 * time.Millisecond)
	if err := storage.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for streams to observe close")
	}
}

func TestReaderDoubleDone(t *testing.T) {
	t.Parallel()

	namespace := "doubledone"
	storage, cleanup := createStorageOpts(t, "./TestReaderDoubleDone",
		[]immuta.OptionFunc{immuta.WithReaderCount(1)}, namespace)
	defer cleanup()

	appendAndSave(t, storage, namespace, []byte("one"))
	appendAndSave(t, storage, namespace, []byte("two"))

	stream := storage.Stream(t.Context(), namespace, 0)
	defer stream.Done()

	r, _, err := stream.Next(t.Context())
	if err != nil {
		t.Fatalf("next failed: %v", err)
	}
	r.Done()
	r.Done() // must not corrupt the fd pool or the stream position

	if got := readNext(t, stream); string(got) != "two" {
		t.Fatalf("expected %q, got %q", "two", got)
	}
}

func TestTransformRoundTrip(t *testing.T) {
	t.Parallel()

	compress := func(r io.Reader) (io.Reader, error) {
		var buf bytes.Buffer
		w, err := flate.NewWriter(&buf, flate.BestSpeed)
		if err != nil {
			return nil, err
		}
		if _, err := io.Copy(w, r); err != nil {
			return nil, err
		}
		if err := w.Close(); err != nil {
			return nil, err
		}
		return &buf, nil
	}
	decompress := func(r io.Reader) (io.Reader, error) {
		return flate.NewReader(r), nil
	}

	namespace := "transform"
	storage, cleanup := createStorageOpts(t, "./TestTransformRoundTrip",
		[]immuta.OptionFunc{
			immuta.WithWriteTransform(compress),
			immuta.WithReadTransform(decompress),
		}, namespace)
	defer cleanup()

	content := bytes.Repeat([]byte("compress me please "), 1000)
	appendAndSave(t, storage, namespace, content)

	stream := storage.Stream(t.Context(), namespace, 0)
	defer stream.Done()

	r, _, err := stream.Next(t.Context())
	if err != nil {
		t.Fatalf("next failed: %v", err)
	}
	defer r.Done()

	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}
	if !bytes.Equal(got, content) {
		t.Fatalf("content mismatch after transform round trip (len %d vs %d)", len(got), len(content))
	}

	if err := storage.Verify(t.Context(), namespace); err != nil {
		t.Fatalf("verify failed: %v", err)
	}
}

func TestSyncOnSave(t *testing.T) {
	t.Parallel()

	namespace := "durable"
	storage, cleanup := createStorageOpts(t, "./TestSyncOnSave",
		[]immuta.OptionFunc{immuta.WithSyncOnSave(true)}, namespace)
	defer cleanup()

	for i := 0; i < 10; i++ {
		appendAndSave(t, storage, namespace, fmt.Appendf(nil, "message-%d", i))
	}

	stream := storage.Stream(t.Context(), namespace, 0)
	defer stream.Done()

	for i := 0; i < 10; i++ {
		expected := fmt.Sprintf("message-%d", i)
		if got := readNext(t, stream); string(got) != expected {
			t.Fatalf("expected %q, got %q", expected, got)
		}
	}

	if err := storage.Verify(t.Context(), namespace); err != nil {
		t.Fatalf("verify failed: %v", err)
	}
}

func TestReopenExistingLog(t *testing.T) {
	t.Parallel()

	namespace := "reopen"
	path := "./TestReopenExistingLog"
	storage, cleanup := createStorage(t, path, namespace)

	appendAndSave(t, storage, namespace, []byte("persisted"))
	storage.Close()

	// reopen the same directory without wiping it
	reopened, err := immuta.New(
		immuta.WithFastWrite(true),
		immuta.WithLogsDirPath(path),
		immuta.WithNamespaces(namespace),
	)
	if err != nil {
		t.Fatalf("failed to reopen storage: %v", err)
	}
	defer cleanup() // removes the directory at the end
	defer reopened.Close()

	appendAndSave(t, reopened, namespace, []byte("appended after reopen"))

	stream := reopened.Stream(t.Context(), namespace, 0)
	defer stream.Done()

	if got := readNext(t, stream); string(got) != "persisted" {
		t.Fatalf("expected %q, got %q", "persisted", got)
	}
	if got := readNext(t, stream); string(got) != "appended after reopen" {
		t.Fatalf("expected %q, got %q", "appended after reopen", got)
	}

	if err := reopened.Verify(t.Context(), namespace); err != nil {
		t.Fatalf("verify failed: %v", err)
	}
}
