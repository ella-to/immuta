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

// FlateCompress returns a Transformer that compresses data using flate.
func FlateCompress(level int) immuta.Transformer {
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
func FlateDecompress() immuta.Transformer {
	return func(r io.Reader) (io.Reader, error) {
		return flate.NewReader(r), nil
	}
}

func main() {
	logDir := "./log-data-compressed"
	poolFileDescriptor := 10
	fastWrite := true
	namespace := "default"

	// Create transformers using flate from the standard library
	compressTransform := FlateCompress(flate.BestSpeed)
	decompressTransform := FlateDecompress()

	log, err := immuta.New(
		immuta.WithLogsDirPath(logDir),
		immuta.WithReaderCount(poolFileDescriptor),
		immuta.WithFastWrite(fastWrite),
		immuta.WithNamespaces(namespace),
		immuta.WithWriteTransform(compressTransform),  // Compress on write
		immuta.WithReadTransform(decompressTransform), // Decompress on read
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
