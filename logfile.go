package immuta

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

// Appender is a single method interface that writes the content of the reader to the storage medium.
type Appender interface {
	// Append writes the content of the reader to the storage medium.
	// and returns the size of the content written.
	Append(r io.Reader) (size int64, err error)
}

// Stream is an interface that deals with reading from the storage medium.
type Stream interface {
	// Creates a io.Reader and provide the size of the content ahead of time.
	Next() (r io.Reader, size int64, err error)
	// Done should be called to release the reader.
	// the best practice is once an stream is created successfully, call Done in defer.
	Done()
}

// HeaderSize is the size of the header that is written before the content.
// Currently the header is 8 bytes long and contains the size of the content.
// However it most cases we don't need that much spaces, the ramining space can be used/reserved
// for other purposes in the future
const HeaderSize = 8

type Storage struct {
	ref       *os.File
	size      int64
	lastIndex int64
	streams   chan *fileReader
}

var _ Appender = (*Storage)(nil)

// Size returns the size of the storage medium.
func (f *Storage) Size() int64 {
	return f.size
}

// LastIndex returns the last index written to the storage medium.
func (f *Storage) LastIndex() int64 {
	return f.lastIndex
}

func (f *Storage) Append(r io.Reader) (size int64, err error) {
	// Need to make space for the header
	err = binary.Write(f.ref, binary.LittleEndian, size)
	if err != nil {
		return -1, err
	}

	f.lastIndex = f.size

	f.size += HeaderSize

	// copy the data and record the size
	size, err = io.Copy(f.ref, r)
	if err != nil {
		return -1, err
	}

	f.size += size

	// Move back to the header and write the size
	_, err = f.ref.Seek(f.size-(size+HeaderSize), io.SeekStart)
	if err != nil {
		return -1, err
	}

	err = binary.Write(f.ref, binary.LittleEndian, size)
	if err != nil {
		return -1, err
	}

	// Move to the end of the file
	_, err = f.ref.Seek(f.size, io.SeekStart)
	if err != nil {
		return -1, err
	}

	return size, nil
}

// Stream creates a reader that reads from the storage medium starting from the index.
// Each object is independent and can be read in any order, multiple readers doesn't affect each other.
// In order to protect the OS from running out of file descriptors, there is a semaphore that limits the number of readers.
// if the semaphore is full, the call will block until a reader is available.
// once the reader is used, Done should be called to release the reader.
// if no options are provided, the reader will start from the beginning of the storage medium.
func (f *Storage) Stream(ctx context.Context, opts ...StreamOpt) (Stream, error) {
	var stream *fileReader

	select {
	case stream = <-f.streams:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	_, err := stream.ref.Seek(0, io.SeekStart)
	if err != nil {
		return nil, err
	}

	for _, opt := range opts {
		if err := opt.configureStream(stream); err != nil {
			return nil, err
		}
	}

	return stream, nil
}

// Close will close all the streams and the storage medium.
// it will wait for all the streams to be closed before closing the storage medium.
func (f *Storage) Close() error {
	// try to close all the streams first,
	for range cap(f.streams) {
		stream := <-f.streams
		err := stream.ref.Close()
		if err != nil {
			return err
		}
	}

	return f.ref.Close()
}

func (f *Storage) stream(index int64) (*fileReader, error) {
	ref, err := os.Open(f.ref.Name())
	if err != nil {
		return nil, err
	}

	_, err = ref.Seek(index, io.SeekStart)
	if err != nil {
		return nil, err
	}

	return &fileReader{
		ref: ref,
	}, nil
}

type StorageOpt interface {
	configureStorage(*Storage) error
}

type storageOptFunc func(*Storage) error

func (f storageOptFunc) configureStorage(s *Storage) error {
	return f(s)
}

type StreamOpt interface {
	configureStream(*fileReader) error
}

type streamOptFunc func(*fileReader) error

func (f streamOptFunc) configureStream(s *fileReader) error {
	return f(s)
}

// WithHighDurability will wait for the data to be fully committed to the storage medium.
// it is slower than WithFastWrite, usually 12-13 times slower.
func WithHighDurability(path string) StorageOpt {
	return storageOptFunc(func(s *Storage) error {
		if s.ref != nil {
			return fmt.Errorf("storage already created")
		}

		// os.O_SYNC is enabled so every write operation will wait for the data
		// to be fully committed to the storage medium before continuing.
		ref, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_SYNC, 0644)
		if err != nil {
			return err
		}

		// check the size of the file
		stat, err := ref.Stat()
		if err != nil {
			return err
		}

		s.size = stat.Size()

		s.ref = ref
		return nil
	})
}

// WithFastWrite will not wait for the data to be fully committed to the storage medium.
// it usually 12-13 times faster than WithHighDurability.
func WithFastWrite(path string) StorageOpt {
	return storageOptFunc(func(s *Storage) error {
		if s.ref != nil {
			return fmt.Errorf("storage already created")
		}

		ref, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			return err
		}

		s.ref = ref

		// check the size of the file
		stat, err := ref.Stat()
		if err != nil {
			return err
		}

		s.size = stat.Size()

		return nil
	})
}

// WithAbsolutePosition will start reading from the index.
// this is useful when you want to keep track of the last read index and start reading from that index.
// make sure to keep track of the size of Header which is 8 bytes
// Recommended to use WithMessageIdx instead.
func WithAbsolutePosition(idx int64) StreamOpt {
	return streamOptFunc(func(s *fileReader) error {
		_, err := s.ref.Seek(idx, io.SeekStart)
		return err
	})
}

// WithRelativeMessage will skip the first count number of messages.
// this is useful when you want to start reading from a specific message.
func WithRelativeMessage(count int64) StreamOpt {
	return streamOptFunc(func(s *fileReader) error {
		var size int64

		for range count {
			err := binary.Read(s.ref, binary.LittleEndian, &size)
			if err != nil {
				return err
			}

			_, err = s.ref.Seek(size, io.SeekCurrent)
			if err != nil {
				return err
			}
		}

		return nil
	})
}

// In order to protect the OS from running out of file descriptors, there is a semaphore that limits the number of readers.
// this will set the size of the semaphore and the number of readers that can be created. the default is 10.
func WithQueueSize(size int) StorageOpt {
	return storageOptFunc(func(s *Storage) error {
		if s.streams != nil {
			return fmt.Errorf("storage already created")
		}

		s.streams = make(chan *fileReader, size)

		return nil
	})
}

// New creates a new storage medium that writes to the file specified by the path.
// The storage medium can be configured with options.
func New(opts ...StorageOpt) (*Storage, error) {
	if opts == nil {
		return nil, fmt.Errorf("no options provided")
	}

	s := &Storage{}

	for _, opt := range opts {
		err := opt.configureStorage(s)
		if err != nil {
			return nil, err
		}
	}

	if s.streams == nil {
		s.streams = make(chan *fileReader, 10)
	}

	for range cap(s.streams) {
		stream, err := s.stream(0)
		if err != nil {
			return nil, err
		}
		stream.done = s.streams
		s.streams <- stream
	}

	return s, nil
}

type fileReader struct {
	ref  *os.File
	done chan<- *fileReader
}

var _ Stream = (*fileReader)(nil)

func (f *fileReader) Next() (r io.Reader, size int64, err error) {
	err = binary.Read(f.ref, binary.LittleEndian, &size)
	if err != nil {
		return nil, 0, err
	}

	return io.LimitReader(f.ref, size), size, nil
}

func (f *fileReader) Done() {
	f.done <- f
}
