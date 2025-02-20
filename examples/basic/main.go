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
