package main

import (
	"bytes"
	"compress/flate"
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"ella.to/immuta"
)

// FlateCompressor implements immuta.Compressor using compress/flate from the standard library.
type FlateCompressor struct {
	level int
}

// NewFlateCompressor creates a new FlateCompressor with the specified compression level.
// level can be from -2 (HuffmanOnly) to 9 (BestCompression).
// Use flate.DefaultCompression (-1) for a balance between speed and compression.
func NewFlateCompressor(level int) *FlateCompressor {
	return &FlateCompressor{level: level}
}

// Compress compresses the data from the reader.
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

// Decompress decompresses the data from the reader.
func (c *FlateCompressor) Decompress(r io.Reader) (io.Reader, error) {
	return flate.NewReader(r), nil
}

func main() {
	logDir := "./log-data-compressed"
	poolFileDescriptor := 10
	fastWrite := true
	namespace := "default"

	// Create a compressor using flate from the standard library
	compressor := NewFlateCompressor(flate.BestSpeed)

	log, err := immuta.New(
		immuta.WithLogsDirPath(logDir),
		immuta.WithReaderCount(poolFileDescriptor),
		immuta.WithFastWrite(fastWrite),
		immuta.WithNamespaces(namespace),
		immuta.WithCompression(compressor), // Enable compression
	)
	if err != nil {
		panic(err)
	}
	defer log.Close()

	// Write some data - it will be automatically compressed
	content := []byte("hello world from compressed storage! This is a longer message to see compression benefits.")

	index, size, err := log.Append(context.Background(), namespace, bytes.NewReader(content))
	if err != nil {
		panic(err)
	}

	fmt.Printf("Original size: %d bytes\n", len(content))
	fmt.Printf("Compressed size: %d bytes (saved %d bytes)\n", size, len(content)-int(size))
	fmt.Printf("Index: %d\n", index)

	// Read the data back - it will be automatically decompressed
	var startPos int64 = 0
	stream := log.Stream(context.Background(), namespace, startPos)
	defer stream.Done()

	err = func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		r, size, err := stream.Next(ctx)
		if err != nil {
			return err
		}
		defer r.Done()

		var buffer bytes.Buffer
		_, err = io.Copy(&buffer, r)
		if err != nil {
			return err
		}

		fmt.Printf("\nRead compressed record size: %d bytes\n", size)
		fmt.Printf("Decompressed content: %s\n", buffer.String())

		if buffer.String() != string(content) {
			return fmt.Errorf("content mismatch: expected %q, got %q", string(content), buffer.String())
		}

		fmt.Println("\n✓ Compression/decompression working correctly!")
		return nil
	}()

	if err != nil && !errors.Is(err, context.DeadlineExceeded) {
		panic(err)
	}
}
