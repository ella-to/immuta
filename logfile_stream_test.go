package immuta_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"ella.to/immuta"
)

func appendBatch(t testing.TB, storage *immuta.Storage, namespace string, contents ...string) {
	t.Helper()
	var err error
	for _, c := range contents {
		if _, _, err = storage.Append(context.Background(), namespace, bytes.NewReader([]byte(c))); err != nil {
			break
		}
	}
	storage.Save(namespace, &err)
	if err != nil {
		t.Fatalf("append batch failed: %v", err)
	}
}

// A stream started at the end must see every record of a later batch commit.
func TestStreamLatestSeesWholeBatch(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name     string
		startPos int64
		existing []string
	}{
		{"latest on non-empty log", -1, []string{"old1", "old2"}},
		{"latest on empty log", -1, nil},
		{"zero on empty log", 0, nil},
		{"past the end", 100, []string{"old1"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			namespace := "latest"
			storage, cleanup := createStorage(t, "./TestStreamLatestSeesWholeBatch/"+tc.name, namespace)
			defer cleanup()

			for _, e := range tc.existing {
				appendAndSave(t, storage, namespace, []byte(e))
			}

			stream := storage.Stream(t.Context(), namespace, tc.startPos)
			defer stream.Done()

			appendBatch(t, storage, namespace, "new1", "new2", "new3")
			appendBatch(t, storage, namespace, "new4")

			for _, want := range []string{"new1", "new2", "new3", "new4"} {
				if got := readNext(t, stream); string(got) != want {
					t.Fatalf("expected %q, got %q", want, got)
				}
			}
		})
	}
}

// Streams created and read while the writer commits must be race free.
func TestStreamConcurrentWithCommits(t *testing.T) {
	t.Parallel()

	namespace := "race"
	storage, cleanup := createStorage(t, "./TestStreamConcurrentWithCommits", namespace)
	defer cleanup()
	appendAndSave(t, storage, namespace, []byte("seed"))

	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 100 {
				stream := storage.Stream(context.Background(), namespace, -1)
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				r, _, err := stream.Next(ctx)
				cancel()
				if err != nil {
					t.Errorf("next failed: %v", err)
				} else {
					r.Done()
				}
				stream.Done()
				_, _ = storage.Details(context.Background(), namespace)
			}
		}()
	}

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	for i := 0; ; i++ {
		select {
		case <-done:
			return
		default:
			appendAndSave(t, storage, namespace, fmt.Appendf(nil, "msg-%d", i))
		}
	}
}

// Done on a stream unblocks a Next waiting in another goroutine.
func TestStreamDoneUnblocksNext(t *testing.T) {
	t.Parallel()

	namespace := "done"
	storage, cleanup := createStorage(t, "./TestStreamDoneUnblocksNext", namespace)
	defer cleanup()

	stream := storage.Stream(t.Context(), namespace, -1)
	errCh := make(chan error, 1)
	go func() {
		_, _, err := stream.Next(context.Background())
		errCh <- err
	}()

	time.Sleep(20 * time.Millisecond)
	stream.Done()

	select {
	case err := <-errCh:
		if !errors.Is(err, immuta.ErrStorageClosed) {
			t.Fatalf("expected ErrStorageClosed, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Next did not return after Done")
	}
}

// Positioning a stream at the end must not depend on the log size.
func TestStreamLatestIsConstantTime(t *testing.T) {
	if testing.Short() {
		t.Skip("writes 200k records")
	}
	t.Parallel()

	namespace := "tail"
	storage, cleanup := createStorage(t, "./TestStreamLatestIsConstantTime", namespace)
	defer cleanup()

	payload := bytes.Repeat([]byte("x"), 64)
	var err error
	for i := range 200_000 {
		if _, _, err = storage.Append(context.Background(), namespace, bytes.NewReader(payload)); err != nil {
			t.Fatal(err)
		}
		if i%1000 == 999 {
			storage.Save(namespace, &err)
		}
	}
	storage.Save(namespace, &err)

	stream := storage.Stream(t.Context(), namespace, -1)
	defer stream.Done()
	appendAndSave(t, storage, namespace, []byte("tail"))

	start := time.Now()
	if got := readNext(t, stream); string(got) != "tail" {
		t.Fatalf("expected %q, got %q", "tail", got)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Millisecond {
		t.Fatalf("first Next at the tail took %v, expected O(1)", elapsed)
	}
}

func TestStreamAtOffsetAndCounters(t *testing.T) {
	t.Parallel()

	namespace := "offset"
	storage, cleanup := createStorage(t, "./TestStreamAtOffsetAndCounters", namespace)
	defer cleanup()

	if got := storage.Namespaces(); len(got) != 1 || got[0] != namespace {
		t.Fatalf("unexpected namespaces %v", got)
	}

	appendBatch(t, storage, namespace, "one", "two", "three")
	if n, _ := storage.Count(namespace); n != 3 {
		t.Fatalf("expected count 3, got %d", n)
	}

	// collect the offset of every record
	stream := storage.Stream(t.Context(), namespace, 0)
	var offsets []int64
	for range 3 {
		r, _, err := stream.Next(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		offsets = append(offsets, r.Offset())
		r.Done()
	}
	stream.Done()

	resumed := storage.StreamAt(t.Context(), namespace, offsets[1])
	defer resumed.Done()
	if got := readNext(t, resumed); string(got) != "two" {
		t.Fatalf("expected %q, got %q", "two", got)
	}

	end, _ := storage.End(namespace)
	tail := storage.StreamAt(t.Context(), namespace, end)
	defer tail.Done()
	appendAndSave(t, storage, namespace, []byte("four"))
	if got := readNext(t, tail); string(got) != "four" {
		t.Fatalf("expected %q, got %q", "four", got)
	}

	for _, off := range []int64{0, end + 1000} {
		_, _, err := storage.StreamAt(t.Context(), namespace, off).Next(t.Context())
		if !errors.Is(err, immuta.ErrInvalidOffset) {
			t.Fatalf("offset %d: expected ErrInvalidOffset, got %v", off, err)
		}
	}
}

func TestStreamUnknownNamespace(t *testing.T) {
	t.Parallel()

	storage, cleanup := createStorage(t, "./TestStreamUnknownNamespace", "known")
	defer cleanup()

	stream := storage.Stream(t.Context(), "unknown", 0)
	defer stream.Done()
	if _, _, err := stream.Next(t.Context()); !errors.Is(err, immuta.ErrNamesapceNotFound) {
		t.Fatalf("expected ErrNamesapceNotFound, got %v", err)
	}
}

// Records of mixed sizes, including ones larger than the read-ahead window
// and ones straddling its boundary, read back intact through every API.
func TestStreamMixedSizesAcrossReadAhead(t *testing.T) {
	t.Parallel()

	namespace := "mixed"
	storage, cleanup := createStorage(t, "./TestStreamMixedSizesAcrossReadAhead", namespace)
	defer cleanup()

	sizes := []int{0, 1, 100, 64*1024 - 8, 64 * 1024, 64*1024 + 1, 3, 200 * 1024, 7, 40 * 1024, 40 * 1024}
	var records [][]byte
	for i, n := range sizes {
		rec := bytes.Repeat([]byte{byte('a' + i)}, n)
		records = append(records, rec)
		appendAndSave(t, storage, namespace, rec)
	}

	viaReader := storage.Stream(t.Context(), namespace, 0)
	defer viaReader.Done()
	viaBytes := storage.Stream(t.Context(), namespace, 0)
	defer viaBytes.Done()

	for i, want := range records {
		if got := readNext(t, viaReader); !bytes.Equal(got, want) {
			t.Fatalf("reader: record %d mismatch (len %d vs %d)", i, len(got), len(want))
		}
		if !viaBytes.Ready() {
			t.Fatalf("bytes: stream should be ready before record %d", i)
		}
		got, _, err := viaBytes.NextBytes(t.Context())
		if err != nil {
			t.Fatalf("bytes: record %d: %v", i, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("bytes: record %d mismatch (len %d vs %d)", i, len(got), len(want))
		}
	}
	if viaBytes.Ready() {
		t.Fatal("stream should not be ready at the end of the log")
	}
}

// A stream waiting at the tail reads each new record as it is committed,
// switching between the small tail window and the pooled read-ahead window.
func TestStreamTailMixedSizes(t *testing.T) {
	t.Parallel()

	namespace := "tailmixed"
	storage, cleanup := createStorage(t, "./TestStreamTailMixedSizes", namespace)
	defer cleanup()

	stream := storage.Stream(t.Context(), namespace, -1)
	defer stream.Done()

	for i, n := range []int{10, 4 * 1024, 4*1024 - 8, 5000, 3, 70 * 1024, 1, 64 * 1024, 0, 2} {
		rec := bytes.Repeat([]byte{byte('a' + i)}, n)
		appendAndSave(t, storage, namespace, rec)
		got, _, err := stream.NextBytes(t.Context())
		if err != nil {
			t.Fatalf("record %d: %v", i, err)
		}
		if !bytes.Equal(got, rec) {
			t.Fatalf("record %d mismatch (len %d vs %d)", i, len(got), n)
		}
	}
}
