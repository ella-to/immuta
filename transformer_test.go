package immuta

import (
	"bytes"
	"compress/flate"
	"context"
	"io"
	"os"
	"testing"
)

// FlateCompress returns a Transformer that compresses data using flate.
func FlateCompress(level int) Transformer {
	return func(r io.Reader) (io.Reader, error) {
		var buf bytes.Buffer
		w, err := flate.NewWriter(&buf, level)
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
}

// FlateDecompress returns a Transformer that decompresses flate data.
func FlateDecompress() Transformer {
	return func(r io.Reader) (io.Reader, error) {
		return flate.NewReader(r), nil
	}
}

func TestTransformerBasic(t *testing.T) {
	path := "./log-data-test-transformer-basic"
	os.RemoveAll(path)
	t.Cleanup(func() {
		os.RemoveAll(path)
	})

	log, err := New(
		WithLogsDirPath(path),
		WithReaderCount(5),
		WithFastWrite(true),
		WithNamespaces("compressed"),
		WithWriteTransform(FlateCompress(flate.BestSpeed)),
		WithReadTransform(FlateDecompress()),
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

func TestTransformerMultipleMessages(t *testing.T) {
	path := "./log-data-test-transformer-multi"
	os.RemoveAll(path)
	t.Cleanup(func() {
		os.RemoveAll(path)
	})

	log, err := New(
		WithLogsDirPath(path),
		WithReaderCount(5),
		WithFastWrite(true),
		WithNamespaces("compressed"),
		WithWriteTransform(FlateCompress(flate.DefaultCompression)),
		WithReadTransform(FlateDecompress()),
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

func TestNoTransformer(t *testing.T) {
	path := "./log-data-test-no-transformer"
	os.RemoveAll(path)
	t.Cleanup(func() {
		os.RemoveAll(path)
	})

	// Verify that not using transformers still works
	log, err := New(
		WithLogsDirPath(path),
		WithReaderCount(5),
		WithFastWrite(true),
		WithNamespaces("uncompressed"),
		// No WithWriteTransform or WithReadTransform
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

// TestChainedTransformers tests that multiple transformers can be chained together
func TestChainedTransformers(t *testing.T) {
	path := "./log-data-test-chained-transformers"
	os.RemoveAll(path)
	t.Cleanup(func() {
		os.RemoveAll(path)
	})

	// Create a simple XOR "encryption" transformer for testing
	xorTransform := func(key byte) Transformer {
		return func(r io.Reader) (io.Reader, error) {
			data, err := io.ReadAll(r)
			if err != nil {
				return nil, err
			}
			for i := range data {
				data[i] ^= key
			}
			return bytes.NewReader(data), nil
		}
	}

	// Chain compression + "encryption"
	writeChain := ChainTransformers(
		FlateCompress(flate.BestSpeed),
		xorTransform(0x42), // XOR with 0x42
	)

	// Chain "decryption" + decompression (reverse order)
	readChain := ChainTransformers(
		xorTransform(0x42), // XOR again to decrypt
		FlateDecompress(),
	)

	log, err := New(
		WithLogsDirPath(path),
		WithReaderCount(5),
		WithFastWrite(true),
		WithNamespaces("chained"),
		WithWriteTransform(writeChain),
		WithReadTransform(readChain),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()

	originalContent := []byte("This is a test message for chained transformers!")
	_, _, err = log.Append(context.Background(), "chained", bytes.NewReader(originalContent))
	if err != nil {
		t.Fatal(err)
	}

	// Read it back
	stream := log.Stream(context.Background(), "chained", 0)
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

	if buf.String() != string(originalContent) {
		t.Fatalf("Content mismatch: expected %q, got %q", string(originalContent), buf.String())
	}
}

// TestChainTransformersWithNil tests that nil transformers in chain are handled
func TestChainTransformersWithNil(t *testing.T) {
	chain := ChainTransformers(
		nil,
		FlateCompress(flate.BestSpeed),
		nil,
	)

	input := bytes.NewReader([]byte("test data"))
	output, err := chain(input)
	if err != nil {
		t.Fatal(err)
	}

	// Should have compressed data
	data, err := io.ReadAll(output)
	if err != nil {
		t.Fatal(err)
	}

	// Decompress and verify
	decompressed := flate.NewReader(bytes.NewReader(data))
	result, err := io.ReadAll(decompressed)
	if err != nil {
		t.Fatal(err)
	}

	if string(result) != "test data" {
		t.Fatalf("expected 'test data', got %q", string(result))
	}
}
