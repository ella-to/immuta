```
██╗███╗░░░███╗███╗░░░███╗██╗░░░██╗████████╗░█████╗░
██║████╗░████║████╗░████║██║░░░██║╚══██╔══╝██╔══██╗
██║██╔████╔██║██╔████╔██║██║░░░██║░░░██║░░░███████║
██║██║╚██╔╝██║██║╚██╔╝██║██║░░░██║░░░██║░░░██╔══██║
██║██║░╚═╝░██║██║░╚═╝░██║╚██████╔╝░░░██║░░░██║░░██║
╚═╝╚═╝░░░░░╚═╝╚═╝░░░░░╚═╝░╚═════╝░░░░╚═╝░░░╚═╝░░╚═╝
```
<div align="center">

[![Go Reference](https://pkg.go.dev/badge/ella.to/immuta.svg)](https://pkg.go.dev/ella.to/immuta)
[![Go Report Card](https://goreportcard.com/badge/ella.to/immuta)](https://goreportcard.com/report/ella.to/immuta)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**Immuta** is a high-performance append-only log implementation based on a single writer, multiple readers architecture. It uses the filesystem as its storage backend and [solid](https://ella.to/solid) for signaling.

</div>

## Features

- **Single Writer, Multiple Readers** - Optimized for append-only workloads with concurrent read access
- **Namespace Isolation** - Data is isolated in separate files per namespace
- **Pluggable Transformers** - Support for compression, encryption, or any custom data transformation
- **Chainable Transformers** - Multiple transformations can be chained together

## File Format

```
+----------+----------+----------+---------------+----------+---------------+
|          |          |          |               |          |               |
| MESSAGES |  LAST    |  PAYLOAD |    PAYLOAD    |  PAYLOAD |    PAYLOAD    | ...
|   COUNT  |  INDEX   |   SIZE   |               |   SIZE   |               |
+----------+----------+----------+---------------+----------+---------------+
   8 bytes   8 bytes    8 bytes                    8 bytes
            (Header)             (Record 1)                  (Record 2)
```

## Installation

```bash
go get ella.to/immuta
```

## Quick Start

```go
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
	// Create storage with a namespace
	log, err := immuta.New(
		immuta.WithLogsDirPath("./log-data"),
		immuta.WithReaderCount(10),      // Pool of file descriptors for readers
		immuta.WithFastWrite(true),      // Use buffered writes (faster but less durable)
		immuta.WithNamespaces("events"), // Create a namespace called "events"
	)
	if err != nil {
		panic(err)
	}
	defer log.Close()

	// Write data
	content := []byte("hello world")
	index, size, err := log.Append(context.Background(), "events", bytes.NewReader(content))
	if err != nil {
		panic(err)
	}
	fmt.Printf("Written at index %d, size %d bytes\n", index, size)

	// Read data using a stream
	stream := log.Stream(context.Background(), "events", 0) // 0 = start from beginning
	defer stream.Done()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	r, size, err := stream.Next(ctx)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			fmt.Println("No more messages")
			return
		}
		panic(err)
	}
	defer r.Done() // Important: release the file descriptor

	var buf bytes.Buffer
	io.Copy(&buf, r)
	fmt.Printf("Read: %s (size: %d)\n", buf.String(), size)
}
```

### Transactional Appends (Save) 🔒

Immuta provides a transactional pattern for appends using the `Save` method. Each append is staged until committed — callers should defer a call to `Save` with a pointer to the returning `err` so the storage can either commit (on success) or roll back (on error).

Example:

```go
func write(ctx context.Context, log *immuta.Storage) (err error) {
    // Defer the Save call immediately so it can commit or rollback based on the named return error.
    defer log.Save("events", &err)

    // Perform the append; Save will commit when this function returns with err == nil
    _, _, err = log.Append(ctx, "events", bytes.NewReader([]byte("some data")))
    if err != nil {
        return err
    }
    return nil
}
```

Notes:

- Call `defer log.Save(namespace, &err)` immediately after you start the operation that performs appends.
- On error, `Save` will truncate the log file back to the previous state.
- On success, `Save` updates the log header and notifies readers of the new messages.

## Configuration Options

| Option | Description | Default |
|--------|-------------|---------|
| `WithLogsDirPath(path)` | Directory for log files | `"./logs"` |
| `WithReaderCount(n)` | Number of pooled file descriptors for concurrent reads | `5` |
| `WithFastWrite(bool)` | Use buffered writes (faster) or sync writes (more durable) | `true` |
| `WithNamespaces(names...)` | Create one or more namespaces | Required |
| `WithWriteTransform(t)` | Transform data before writing | `nil` |
| `WithReadTransform(t)` | Transform data after reading | `nil` |

## Stream Positioning

When creating a stream, the `startPos` parameter controls where reading begins:

```go
// Start from the beginning (read all messages)
stream := log.Stream(ctx, "events", 0)

// Start from the latest (only new messages)
stream := log.Stream(ctx, "events", -1)

// Skip the first N messages
stream := log.Stream(ctx, "events", 10) // Skip first 10 messages
```

## Data Transformers

Transformers allow you to modify data as it's written or read. Common use cases include compression and encryption.

### Transformer Type

```go
type Transformer func(r io.Reader) (io.Reader, error)
```

### Compression Example

```go
import (
	"bytes"
	"compress/flate"
	"io"
	"ella.to/immuta"
)

// Compress transforms data by compressing it
func Compress(level int) immuta.Transformer {
	return func(r io.Reader) (io.Reader, error) {
		var buf bytes.Buffer
		w, err := flate.NewWriter(&buf, level)
		if err != nil {
			return nil, err
		}
		if _, err := io.Copy(w, r); err != nil {
			w.Close()
			return nil, err
		}
		if err := w.Close(); err != nil {
			return nil, err
		}
		return &buf, nil
	}
}

// Decompress transforms data by decompressing it
func Decompress() immuta.Transformer {
	return func(r io.Reader) (io.Reader, error) {
		return flate.NewReader(r), nil
	}
}

func main() {
	log, _ := immuta.New(
		immuta.WithLogsDirPath("./logs"),
		immuta.WithNamespaces("compressed"),
		immuta.WithWriteTransform(Compress(flate.BestSpeed)),
		immuta.WithReadTransform(Decompress()),
	)
	defer log.Close()
	
	// Data is automatically compressed on write and decompressed on read
}
```

### Chaining Transformers

Multiple transformers can be chained together. They are applied in order:

```go
// Chain compression and then encryption on write
writeChain := immuta.ChainTransformers(
	Compress(flate.BestSpeed),
	Encrypt(key),
)

// Chain decryption and then decompression on read (reverse order)
readChain := immuta.ChainTransformers(
	Decrypt(key),
	Decompress(),
)

log, _ := immuta.New(
	immuta.WithLogsDirPath("./logs"),
	immuta.WithNamespaces("secure"),
	immuta.WithWriteTransform(writeChain),
	immuta.WithReadTransform(readChain),
)
```

### Using Third-Party Libraries

You can use any compression or encryption library by wrapping it in a `Transformer`:

```go
import "github.com/klauspost/compress/s2"

func S2Compress() immuta.Transformer {
	return func(r io.Reader) (io.Reader, error) {
		var buf bytes.Buffer
		w := s2.NewWriter(&buf)
		if _, err := io.Copy(w, r); err != nil {
			w.Close()
			return nil, err
		}
		if err := w.Close(); err != nil {
			return nil, err
		}
		return &buf, nil
	}
}

func S2Decompress() immuta.Transformer {
	return func(r io.Reader) (io.Reader, error) {
		return s2.NewReader(r), nil
	}
}
```

## Error Handling

Immuta provides specific error types:

```go
var (
	ErrNamespaceRequired = errors.New("namespace is required")
	ErrNamesapceNotFound = errors.New("namespace not found")
	ErrStorageClosed     = errors.New("storage is closed")
)
```

When storage is closed, any blocked `stream.Next()` calls will unblock and return `ErrStorageClosed`.

## Complete Example

```go
package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"ella.to/immuta"
)

func main() {
	log, err := immuta.New(
		immuta.WithLogsDirPath("./log-data"),
		immuta.WithReaderCount(10),
		immuta.WithFastWrite(true),
		immuta.WithNamespaces("events"),
	)
	if err != nil {
		panic(err)
	}
	defer log.Close()

	var wg sync.WaitGroup

	// Writer goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			msg := fmt.Sprintf("message %d", i)
			_, _, err := log.Append(context.Background(), "events", bytes.NewReader([]byte(msg)))
			if err != nil {
				fmt.Printf("Write error: %v\n", err)
				return
			}
		}
	}()

	// Reader goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		stream := log.Stream(context.Background(), "events", 0)
		defer stream.Done()

		count := 0
		for count < 100 {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			r, _, err := stream.Next(ctx)
			cancel()

			if errors.Is(err, context.DeadlineExceeded) {
				break
			}
			if err != nil {
				fmt.Printf("Read error: %v\n", err)
				return
			}

			var buf bytes.Buffer
			io.Copy(&buf, r)
			r.Done()

			count++
		}
		fmt.Printf("Read %d messages\n", count)
	}()

	wg.Wait()
}
```

## Performance

Benchmarks on Apple M2 Pro:

```bash
go test -bench=. -benchmem
```

### Write Performance

| Message Size | Throughput | Allocations |
|--------------|------------|-------------|
| 100 bytes | 18.30 MB/s | 2 allocs/op |
| 1 KB | 162.09 MB/s | 2 allocs/op |
| 4 KB | 421.89 MB/s | 2 allocs/op |
| 64 KB | 922.17 MB/s | 2 allocs/op |

### Read Performance

| Message Size | Throughput | Allocations |
|--------------|------------|-------------|
| 1 KB | 800.07 MB/s | 5 allocs/op |

### Bulk Operations

- Writing 100k records of 1KB: ~1.2 seconds
- Reading 100k records of 1KB: ~335ms

```bash
# Run benchmarks
go test -bench=. -benchmem

# Run with race detector
go test -race ./...
```

## Thread Safety

- **Append**: Should be called from a single goroutine (not safe for concurrent writes)
- **Stream**: Safe for concurrent use; multiple streams can read simultaneously
- **Close**: Safe to call multiple times; properly unblocks waiting streams

## License

MIT License - see [LICENSE](LICENSE) for details.
