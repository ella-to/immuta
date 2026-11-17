package immuta_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"ella.to/immuta"
)

func reopen(t *testing.T, path string, namespace string) *immuta.Storage {
	t.Helper()
	storage, err := immuta.New(
		immuta.WithLogsDirPath(path),
		immuta.WithNamespaces(namespace),
	)
	if err != nil {
		t.Fatalf("failed to reopen storage: %v", err)
	}
	t.Cleanup(func() { storage.Close() })
	return storage
}

func expectRecords(t *testing.T, storage *immuta.Storage, namespace string, want ...string) {
	t.Helper()
	stream := storage.Stream(t.Context(), namespace, 0)
	defer stream.Done()
	for _, w := range want {
		if got := readNext(t, stream); string(got) != w {
			t.Fatalf("expected %q, got %q", w, got)
		}
	}
	if err := storage.Verify(t.Context(), namespace); err != nil {
		t.Fatalf("verify failed: %v", err)
	}
}

// An Append that was never saved must not be visible after Close + reopen.
func TestCloseDropsUnsavedAppend(t *testing.T) {
	t.Parallel()

	namespace := "unsaved"
	path := "./TestCloseDropsUnsavedAppend"
	storage, cleanup := createStorage(t, path, namespace)
	defer cleanup()

	appendAndSave(t, storage, namespace, []byte("one"))
	appendAndSave(t, storage, namespace, []byte("two"))
	if _, _, err := storage.Append(t.Context(), namespace, bytes.NewReader([]byte("orphan"))); err != nil {
		t.Fatalf("append failed: %v", err)
	}
	storage.Close()

	reopened := reopen(t, path, namespace)
	appendAndSave(t, reopened, namespace, []byte("three"))
	expectRecords(t, reopened, namespace, "one", "two", "three")
}

// Bytes left behind by a crash between Append and Save are truncated on open.
func TestReopenTruncatesUncommittedTail(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name      string
		committed []string
	}{
		{"empty", nil},
		{"non-empty", []string{"one", "two"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			namespace := "crash"
			path := filepath.Join("./TestReopenTruncatesUncommittedTail", tc.name)
			storage, cleanup := createStorage(t, path, namespace)
			defer cleanup()

			for _, c := range tc.committed {
				appendAndSave(t, storage, namespace, []byte(c))
			}
			storage.Close()

			// simulate a crash after a record was written but before Save
			f, err := os.OpenFile(filepath.Join(path, namespace+".log"), os.O_WRONLY|os.O_APPEND, 0)
			if err != nil {
				t.Fatal(err)
			}
			orphan := binary.LittleEndian.AppendUint64(nil, 6)
			orphan = append(orphan, "orphan"...)
			if _, err := f.Write(orphan); err != nil {
				t.Fatal(err)
			}
			f.Close()

			reopened := reopen(t, path, namespace)
			appendAndSave(t, reopened, namespace, []byte("three"))
			expectRecords(t, reopened, namespace, append(tc.committed, "three")...)
		})
	}
}

// A header that points outside the file is reported instead of trusted.
func TestReopenDetectsCorruptHeader(t *testing.T) {
	t.Parallel()

	namespace := "corrupt"
	path := "./TestReopenDetectsCorruptHeader"
	storage, cleanup := createStorage(t, path, namespace)
	defer cleanup()

	appendAndSave(t, storage, namespace, []byte("one"))
	storage.Close()

	f, err := os.OpenFile(filepath.Join(path, namespace+".log"), os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt(binary.LittleEndian.AppendUint64(nil, 1<<40), 8); err != nil {
		t.Fatal(err)
	}
	f.Close()

	_, err = immuta.New(immuta.WithLogsDirPath(path), immuta.WithNamespaces(namespace))
	if !errors.Is(err, immuta.ErrCorrupted) {
		t.Fatalf("expected ErrCorrupted, got %v", err)
	}
}

// A record size that points past the committed data is reported, not trusted.
func TestNextDetectsCorruptRecordSize(t *testing.T) {
	t.Parallel()

	namespace := "corruptsize"
	path := "./TestNextDetectsCorruptRecordSize"
	storage, cleanup := createStorage(t, path, namespace)
	defer cleanup()

	appendAndSave(t, storage, namespace, []byte("one"))
	appendAndSave(t, storage, namespace, []byte("two"))

	// corrupt the size of the first record in place
	f, err := os.OpenFile(filepath.Join(path, namespace+".log"), os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt(binary.LittleEndian.AppendUint64(nil, 1<<62), immuta.FileHeaderSize); err != nil {
		t.Fatal(err)
	}
	f.Close()

	for _, start := range []int64{0, 1} {
		stream := storage.Stream(t.Context(), namespace, start)
		_, _, err = stream.Next(t.Context())
		stream.Done()
		if !errors.Is(err, immuta.ErrCorrupted) {
			t.Fatalf("start %d: expected ErrCorrupted, got %v", start, err)
		}
	}
	if err := storage.Verify(t.Context(), namespace); !errors.Is(err, immuta.ErrCorrupted) {
		t.Fatalf("verify: expected ErrCorrupted, got %v", err)
	}
}
