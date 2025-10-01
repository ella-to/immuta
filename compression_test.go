package immuta

import (
	"bytes"
	"compress/flate"
	"context"
	"io"
	"os"
	"testing"
)

// FlateCompressor implements Compressor using compress/flate from the standard library.
type FlateCompressor struct {
	level int
}

func NewFlateCompressor(level int) *FlateCompressor {
	return &FlateCompressor{level: level}
}

func (c *FlateCompressor) Compress(r io.Reader) (io.Reader, error) {
	var buf bytes.Buffer
	w, err := flate.NewWriter(&buf, c.level)
	if err != nil {
		return nil, err
	}

	_, err = io.Copy(w, r)
	if err != nil {
		w.Close()
		return nil, err
	}

	err = w.Close()
	if err != nil {
		return nil, err
	}

	return &buf, nil
}

func (c *FlateCompressor) Decompress(r io.Reader) (io.Reader, error) {
	return flate.NewReader(r), nil
}

func TestCompressionBasic(t *testing.T) {
	path := "./log-data-test-compression-basic"
	os.RemoveAll(path)
	t.Cleanup(func() {
		os.RemoveAll(path)
	})

	compressor := NewFlateCompressor(flate.BestSpeed)

	log, err := New(
		WithLogsDirPath(path),
		WithReaderCount(5),
		WithFastWrite(true),
		WithNamespaces("compressed"),
		WithCompression(compressor),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()

	// Write some data
	originalContent := []byte("hello world from compressed test! This message should be compressed.")
	_, compressedSize, err := log.Append(context.Background(), "compressed", bytes.NewReader(originalContent))
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("Original size: %d, Compressed size: %d", len(originalContent), compressedSize)

	// Verify compressed size is smaller than original (for this data)
	if compressedSize >= int64(len(originalContent)) {
		t.Logf("Warning: Compressed size (%d) is not smaller than original (%d). This can happen with small or incompressible data.", compressedSize, len(originalContent))
	}

	// Read it back
	stream := log.Stream(context.Background(), "compressed", 0)
	defer stream.Done()

	r, _, err := stream.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer r.Done()

	var buf bytes.Buffer
	_, err = io.Copy(&buf, r)
	if err != nil {
		t.Fatal(err)
	}

	// Verify content matches
	if buf.String() != string(originalContent) {
		t.Fatalf("Content mismatch: expected %q, got %q", string(originalContent), buf.String())
	}
}

func TestCompressionMultipleMessages(t *testing.T) {
	path := "./log-data-test-compression-multi"
	os.RemoveAll(path)
	t.Cleanup(func() {
		os.RemoveAll(path)
	})

	compressor := NewFlateCompressor(flate.DefaultCompression)

	log, err := New(
		WithLogsDirPath(path),
		WithReaderCount(5),
		WithFastWrite(true),
		WithNamespaces("compressed"),
		WithCompression(compressor),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()

	// Write multiple messages
	messages := []string{
		"First message with some content to compress",
		"Second message with different content",
		"Third message, also compressed independently",
	}

	for _, msg := range messages {
		_, _, err := log.Append(context.Background(), "compressed", bytes.NewReader([]byte(msg)))
		if err != nil {
			t.Fatal(err)
		}
	}

	// Read them back
	stream := log.Stream(context.Background(), "compressed", 0)
	defer stream.Done()

	for i, expectedMsg := range messages {
		r, _, err := stream.Next(context.Background())
		if err != nil {
			t.Fatal(err)
		}

		var buf bytes.Buffer
		_, err = io.Copy(&buf, r)
		r.Done()
		if err != nil {
			t.Fatal(err)
		}

		if buf.String() != expectedMsg {
			t.Fatalf("Message %d mismatch: expected %q, got %q", i, expectedMsg, buf.String())
		}
	}
}

func TestNoCompression(t *testing.T) {
	path := "./log-data-test-no-compression"
	os.RemoveAll(path)
	t.Cleanup(func() {
		os.RemoveAll(path)
	})

	// Verify that not using compression still works
	log, err := New(
		WithLogsDirPath(path),
		WithReaderCount(5),
		WithFastWrite(true),
		WithNamespaces("uncompressed"),
		// No WithCompression call
	)
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()

	content := []byte("hello world without compression")
	_, size, err := log.Append(context.Background(), "uncompressed", bytes.NewReader(content))
	if err != nil {
		t.Fatal(err)
	}

	// Size should match original since there's no compression
	if size != int64(len(content)) {
		t.Fatalf("Size mismatch: expected %d, got %d", len(content), size)
	}

	// Read it back
	stream := log.Stream(context.Background(), "uncompressed", 0)
	defer stream.Done()

	r, _, err := stream.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer r.Done()

	var buf bytes.Buffer
	_, err = io.Copy(&buf, r)
	if err != nil {
		t.Fatal(err)
	}

	if buf.String() != string(content) {
		t.Fatalf("Content mismatch: expected %q, got %q", string(content), buf.String())
	}
}
