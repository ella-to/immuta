```
██╗███╗░░░███╗███╗░░░███╗██╗░░░██╗████████╗░█████╗░
██║████╗░████║████╗░████║██║░░░██║╚══██╔══╝██╔══██╗
██║██╔████╔██║██╔████╔██║██║░░░██║░░░██║░░░███████║
██║██║╚██╔╝██║██║╚██╔╝██║██║░░░██║░░░██║░░░██╔══██║
██║██║░╚═╝░██║██║░╚═╝░██║╚██████╔╝░░░██║░░░██║░░██║
╚═╝╚═╝░░░░░╚═╝╚═╝░░░░░╚═╝░╚═════╝░░░░╚═╝░░░╚═╝░░╚═╝
```

Immuta is a `Append Only Log` implementation based on single writer, multiple readers concept. It uses filesystem as it's core the format of the each record is as follows and uses [solid](https://ella.to/solid) for io signgling

- the first 8 bytes provide the number of messages in the log file
- loop
  - the next 8 bytes define the size of the payload (Header)
  - payload can be any arbitrary size

```
+----------+----------+---------------+----------+---------------+
|          |          |               |          |               |
| MESSAGES |  PAYLOAD |    PAYLOAD    |  PAYLOAD |    PAYLOAD    | ...
|   COUNT  |   SIZE   |               |   SIZE   |               |
+----------+----------+---------------+----------+---------------+
   8 bytes   8 bytes                    8 bytes
```

# Installation

```bash
go get ella.to/immuta
```

# Usgae

```golang
package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"ella.to/immuta"
)

func main() {
	logDir := "./log-data"
	poolFileDescriptor := 10
	// fastwrite uses the buffer for each append
	// if you need gurrantee on saving on disk, enable set fastWrite to false
	// the Append operation will get the performance hit
	fastWrite := true

	// namespace is isolating the data in it's own file which managed by immuta
	namespace := "default"

	log, err := immuta.New(
		immuta.WithLogsDirPath(logDir),
		immuta.WithReaderCount(poolFileDescriptor),
		immuta.WithFastWrite(fastWrite),
		immuta.WithNamespaces(namespace),
	)
	if err != nil {
		panic(err)
	}
	defer log.Close()

	content := []byte("hello world")

	// write to append only log
	index, size, err := log.Append(context.Background(), namespace, bytes.NewReader(content))
	if err != nil {
		panic(err)
	}

	if index != 8 {
		panic("index must be 8")
	}

	if size != 11 {
		panic("size must be 11")
	}

	// 0: start from beginning
	// negative value: start from latest append
	// positive number: skip those message
	var startPos int64 = 0

	// this call doesn't allocate any file descriptor yet
	stream := log.Stream(context.Background(), namespace, startPos)
	defer stream.Done()

	for {
		var buffer bytes.Buffer

		err := func() error {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			r, size, err := stream.Next(ctx)
			if err != nil {
				return err
			}
			// important: don't forget to call Done() to release the file descriptor
			defer r.Done()

			buffer.Reset()

			_, err = io.Copy(&buffer, r)
			if err != nil {
				return err
			}

			fmt.Printf("size of the record: %d\n", size)
			fmt.Printf("content: %s\n", buffer.String())

			return nil
		}()

		if errors.Is(err, context.DeadlineExceeded) {
			break
		} else if err != nil {
			panic(err)
		}
	}
}
```

# Compression

The `immuta` library supports pluggable compression through the `Compressor` interface. This allows you to use any compression algorithm you prefer, such as:

- Standard library algorithms (flate, gzip, zlib, lzw)
- Third-party algorithms (s2, snappy, zstd, lz4, etc.)

## Compressor Interface

To implement compression, you need to provide a type that implements the `Compressor` interface:

```go
type Compressor interface {
    // Compress takes a reader and returns a reader that compresses the data.
    Compress(r io.Reader) (io.Reader, error)
    // Decompress takes a reader and returns a reader that decompresses the data.
    Decompress(r io.Reader) (io.Reader, error)
}
```

## Example Implementation

This example shows how to implement the `Compressor` interface using `compress/flate` from the standard library:

```go
type FlateCompressor struct {
    level int
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
```

## Usage

To enable compression, simply pass your compressor implementation to the `WithCompression` option:

```go
compressor := NewFlateCompressor(flate.BestSpeed)

log, err := immuta.New(
    immuta.WithLogsDirPath(logDir),
    immuta.WithReaderCount(poolFileDescriptor),
    immuta.WithFastWrite(fastWrite),
    immuta.WithNamespaces(namespace),
    immuta.WithCompression(compressor), // Enable compression
)
```

Once configured, all data written via `Append()` will be automatically compressed, and all data read via `Stream()` will be automatically decompressed. The compression is transparent to your application code.

## Using Third-Party Compression Libraries

You can use any compression library by implementing the `Compressor` interface. For example, to use `github.com/klauspost/compress/s2`:

```go
import "github.com/klauspost/compress/s2"

type S2Compressor struct{}

func (c *S2Compressor) Compress(r io.Reader) (io.Reader, error) {
    var buf bytes.Buffer
    w := s2.NewWriter(&buf)
    
    _, err := io.Copy(w, r)
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

func (c *S2Compressor) Decompress(r io.Reader) (io.Reader, error) {
    return s2.NewReader(r), nil
}
```

# Performance

- Appending 100k records of 1kb took around 1 seconds

```
go test -benchmem -run=^$ -bench ^Benchmark1kbAppend$ ella.to/immuta

goos: darwin
goarch: arm64
pkg: ella.to/immuta
cpu: Apple M2 Pro
Benchmark1kbAppend-12             119136             10111 ns/op              56 B/op          2 allocs/op
```

- Reading the 100k record is under 150ms

```
go test -timeout 30s -run ^TestRead100kMessages$ ella.to/immuta -v
=== RUN   TestRead100kMessages
=== PAUSE TestRead100kMessages
=== CONT  TestRead100kMessages
time taken to write 100000: 925.566167ms
time taken to read 100000: 148.137541ms
--- PASS: TestRead100kMessages (1.07s)
```
