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
