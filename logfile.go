package immuta

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync/atomic"

	"ella.to/solid"
)

const (
	HeaderSize = 8
)

var (
	emptyHeader = make([]byte, HeaderSize)
)

// Appender is a single method interface that writes the content of the reader to the storage medium.
type Appender interface {
	// Append writes the content of the reader to the storage medium.
	// and returns the index and size of the content written.
	Append(ctx context.Context, r io.Reader) (index int64, size int64, err error)
}

// Stream is an interface that deals with reading from the storage medium.
type Stream interface {
	// Creates a io.Reader and provide the size of the content ahead of time.
	// If there is no more content to read, it will blocked until there is more content or the context is done.
	Next(ctx context.Context) (r io.Reader, size int64, err error)
	// Done should be called to release the reader.
	// the best practice is once an stream is created successfully, call Done in defer.
	Done()
}

//
// Storage
//

type Storage struct {
	w             *os.File
	fds           chan *os.File
	bc            *solid.Broadcast
	currSize      int64
	currCount     int64
	streamIdcount atomic.Int64
}

var _ Appender = (*Storage)(nil)

func (s *Storage) getFd(ctx context.Context) (*os.File, error) {
	select {
	case fd, ok := <-s.fds:
		if !ok {
			return nil, fmt.Errorf("no more file descriptors")
		}
		return fd, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (s *Storage) putFd(ctx context.Context, fd *os.File) error {
	select {
	case s.fds <- fd:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Storage) Details() (string, error) {
	fd, err := s.getFd(context.Background())
	if err != nil {
		return "", err
	}
	defer s.putFd(context.Background(), fd)

	stat, err := fd.Stat()
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("size: %d, count: %d", stat.Size(), s.currCount), nil
}

func (s *Storage) Verify() error {
	fd, err := s.getFd(context.Background())
	if err != nil {
		return err
	}
	defer s.putFd(context.Background(), fd)

	stat, err := fd.Stat()
	if err != nil {
		return err
	}

	size := stat.Size()

	var total int64
	err = binary.Read(fd, binary.LittleEndian, &total)
	if err != nil {
		return err
	}

	var calculateSize int64 = HeaderSize

	for range total {
		var contentSize int64
		err = binary.Read(fd, binary.LittleEndian, &contentSize)
		if err != nil {
			return err
		}

		_, err = fd.Seek(contentSize, io.SeekCurrent)
		if err != nil {
			return err
		}

		calculateSize += contentSize + HeaderSize
	}

	if size != calculateSize {
		return fmt.Errorf("size mismatch: expected %d, got %d", calculateSize, size)
	}

	return nil
}

func (s *Storage) Append(ctx context.Context, r io.Reader) (index int64, size int64, err error) {
	defer func() {
		var shouldSignal bool

		// if there is an error, move the current position back to the original position
		if err == nil {
			s.currSize += size + HeaderSize
			s.currCount++

			_, err = s.w.Seek(0, io.SeekStart)
			if err != nil {
				slog.Error("failed to seek to the beginning of the file", "err", err)
			} else {
				err = binary.Write(s.w, binary.LittleEndian, s.currCount)
				if err != nil {
					slog.Error("failed to write the size of the content", "err", err)
				} else {
					shouldSignal = true
				}
			}

			if shouldSignal {
				s.bc.Notify()
			}

		} else {
			// truncate the file to the original size
			err = s.w.Truncate(s.currSize)
			if err != nil {
				slog.Error("failed to truncate the file", "err", err, "size", s.currSize)
			}
		}
	}()

	index = s.currSize

	_, err = s.w.Seek(s.currSize, io.SeekStart)
	if err != nil {
		return -1, -1, err
	}

	// write header first
	//
	_, err = s.w.Write(emptyHeader)
	if err != nil {
		return -1, -1, err
	}

	// copy the content
	//
	size, err = io.Copy(s.w, r)
	if err != nil {
		return -1, -1, err
	}

	currSize := s.currSize + size + HeaderSize

	// Move back to the header of the content
	//
	_, err = s.w.Seek(currSize-(size+HeaderSize), io.SeekStart)
	if err != nil {
		return -1, -1, err
	}

	// write the size of the content
	//
	err = binary.Write(s.w, binary.LittleEndian, size)
	if err != nil {
		return -1, -1, err
	}

	return index, size, nil
}

func (s *Storage) Close() error {
	for range cap(s.fds) {
		fd := <-s.fds
		if err := fd.Close(); err != nil {
			return err
		}
	}
	return s.w.Close()
}

func (s *Storage) getIndexFromPos(ctx context.Context, startPos int64) (int64, error) {
	fd, err := s.getFd(ctx)
	if err != nil {
		return -1, err
	}
	defer s.putFd(context.WithoutCancel(ctx), fd)

	// moved to the beginning of the file
	// to read the total number of messages

	fd.Seek(0, io.SeekStart)

	var total int64
	err = binary.Read(fd, binary.LittleEndian, &total)
	if err != nil {
		return -1, err
	}

	// if startPos is negative, start from the latest messages

	if startPos < 0 || total-startPos <= 0 {
		stat, err := fd.Stat()
		if err != nil {
			return -1, err
		}
		return stat.Size(), nil
	} else if startPos == 0 {
		return HeaderSize, nil
	}

	var index int64 = HeaderSize

	for range startPos {
		var size int64
		err = binary.Read(fd, binary.LittleEndian, &size)
		if err != nil {
			return -1, err
		}

		_, err = fd.Seek(size, io.SeekCurrent)
		if err != nil {
			return -1, err
		}

		index += size + HeaderSize
	}

	return index, nil
}

// Stream(ctx, 0) 	-> from the beginning
// Stream(ctx, -1) 	-> start from latest messages
// Stream(ctx, 10) 	-> start after 10nth message
func (s *Storage) Stream(ctx context.Context, startPos int64) (Stream, error) {
	index, err := s.getIndexFromPos(ctx, startPos)
	if err != nil {
		return nil, err
	}

	return &stream{
		id:     s.streamIdcount.Add(1),
		index:  index,
		getFd:  s.getFd,
		putFd:  s.putFd,
		signal: s.bc.CreateSignal(solid.WithHistory(startPos)),
	}, nil
}

func New(filepath string, readerCount int, fastWrite bool) (*Storage, error) {
	if readerCount <= 0 {
		return nil, fmt.Errorf("readerCount must be greater than 0")
	}

	var flag int

	if fastWrite {
		flag = os.O_RDWR | os.O_CREATE
	} else {
		flag = os.O_RDWR | os.O_CREATE | os.O_SYNC
	}

	w, err := os.OpenFile(filepath, flag, 0644)
	if err != nil {
		return nil, err
	}

	size, err := getSize(w)
	if err != nil {
		return nil, err
	}

	if size == 0 {
		_, err = w.Write(emptyHeader)
		if err != nil {
			return nil, err
		}
	}

	_, err = w.Seek(0, io.SeekStart)
	if err != nil {
		return nil, err
	}

	//
	// read the first 8 bytes to get the number of messages
	//

	var count int64
	err = binary.Read(w, binary.LittleEndian, &count)
	if err != nil {
		return nil, err
	}

	// move the cursor to the end of the file

	size, err = getSize(w)
	if err != nil {
		return nil, err
	}

	_, err = w.Seek(size, io.SeekStart)
	if err != nil {
		return nil, err
	}

	storage := &Storage{
		w:         w,
		fds:       make(chan *os.File, readerCount),
		bc:        solid.NewBroadcast(),
		currSize:  size,
		currCount: count,
	}

	for i := 0; i < readerCount; i++ {
		fd, err := os.Open(filepath)
		if err != nil {
			return nil, err
		}
		storage.fds <- fd
	}

	return storage, nil
}

func getSize(w *os.File) (int64, error) {
	stat, err := w.Stat()
	if err != nil {
		return -1, err
	}
	return stat.Size(), nil
}

//
// stream
//

type stream struct {
	id     int64
	index  int64
	getFd  func(context.Context) (*os.File, error)
	putFd  func(context.Context, *os.File) error
	signal *solid.Signal
}

func (s *stream) String() string {
	return fmt.Sprintf("stream-%d -> %d", s.id, s.index)
}

var _ Stream = (*stream)(nil)

func (s *stream) Next(ctx context.Context) (r io.Reader, size int64, err error) {
CHECK:
	err = s.signal.Wait(ctx)
	if err != nil {
		return nil, -1, err
	}

	fd, err := s.getFd(ctx)
	if err != nil {
		return nil, -1, err
	}

	_, err = fd.Seek(s.index, io.SeekStart)
	if err != nil {
		return nil, -1, fmt.Errorf("failed to seek to index %d, id: %d: %w", s.index, s.id, err)
	}

	err = binary.Read(fd, binary.LittleEndian, &size)
	if errors.Is(err, io.EOF) {

		goto CHECK
	} else if err != nil {
		return nil, -1, fmt.Errorf("failed to read size of content at %d, id: %d: %w", s.index, s.id, err)
	}

	return &reader{
		r: io.LimitReader(fd, size),
		done: func() {
			s.index += size + HeaderSize
			err = s.putFd(ctx, fd)
			if err != nil {
				slog.Error("failed to return the file descriptor back to the pool", "err", err)
			}
		},
	}, size, nil
}

func (s *stream) Done() {
	s.signal.Done()
}

//
// reader
//

type reader struct {
	r    io.Reader
	done func()
}

var _ io.Reader = (*reader)(nil)

func (r *reader) Read(p []byte) (n int, err error) {
	n, err = r.r.Read(p)
	if err != nil {
		r.done()
	}
	return
}
