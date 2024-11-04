package immuta

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sync/atomic"

	"ella.to/solid"
)

const (
	FileHeaderSize   = 8 + 8 // total number of messages + index of the last message
	RecordHeaderSize = 8     // size of the content of the message
)

var (
	emptyFileHeader   = make([]byte, FileHeaderSize)
	emptyRecordHeader = make([]byte, RecordHeaderSize)
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
	Next(ctx context.Context) (r *Reader, size int64, err error)
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
	lastIndex     int64
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

func loadFileHeader(fd *os.File) (total int64, index int64, err error) {
	_, err = fd.Seek(0, io.SeekStart)
	if err != nil {
		return -1, -1, err
	}

	err = binary.Read(fd, binary.LittleEndian, &total)
	if err != nil {
		return -1, -1, err
	}

	err = binary.Read(fd, binary.LittleEndian, &index)
	if err != nil {
		return -1, -1, err
	}

	return total, index, nil
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

	total, lastIndex, err := loadFileHeader(fd)

	var calculateSize int64 = FileHeaderSize

	var currIndex int64

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
		currIndex = calculateSize
		calculateSize += contentSize + RecordHeaderSize
	}

	if size != calculateSize {
		return fmt.Errorf("size mismatch: expected %d, got %d", calculateSize, size)
	}

	if lastIndex != currIndex {
		return fmt.Errorf("last index mismatch: expected %d, got %d", currIndex, lastIndex)
	}

	return nil
}

// Append should only be called by a single goroutine at a time. It is not safe for concurrent use.
func (s *Storage) Append(ctx context.Context, r io.Reader) (index int64, size int64, err error) {
	defer func() {
		// if there is an error, move the current position back to the original position
		if err != nil {
			// truncate the file to the original size
			err = s.w.Truncate(s.currSize)
			if err != nil {
				err = fmt.Errorf("failed to truncate the file: %w", err)
			}

			return
		}

		_, err = s.w.Seek(0, io.SeekStart)
		if err != nil {
			err = fmt.Errorf("failed to seek to the beginning of the file: %w", err)
			return
		}

		currCount := s.currCount + 1

		// write the total number of messages and the index of the last message
		// into a single 16 bytes and write it to the beginning of the file
		// to make sure that the file is consistent and if the file is corrupted
		// revert back to the original state.
		var header [16]byte
		binary.LittleEndian.PutUint64(header[:8], uint64(currCount))
		binary.LittleEndian.PutUint64(header[8:], uint64(index))

		_, err = s.w.Write(header[:])
		if err != nil {
			err = fmt.Errorf("failed to write the header: %w", err)
			return
		}

		// only update the current size and the last index if there is no error
		s.currSize += size + RecordHeaderSize
		s.lastIndex = index
		s.currCount = currCount

		s.bc.Notify()
	}()

	index = s.currSize

	_, err = s.w.Seek(s.currSize, io.SeekStart)
	if err != nil {
		return -1, -1, err
	}

	// write header first
	//
	_, err = s.w.Write(emptyRecordHeader)
	if err != nil {
		return -1, -1, err
	}

	// copy the content
	//
	size, err = io.Copy(s.w, r)
	if err != nil {
		return -1, -1, err
	}

	currSize := s.currSize + size + RecordHeaderSize

	// Move back to the header of the content
	//
	_, err = s.w.Seek(currSize-(size+RecordHeaderSize), io.SeekStart)
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

// Stream(ctx, 0) 	-> from the beginning
// Stream(ctx, -1) 	-> start from latest messages
// Stream(ctx, 10) 	-> start after 10nth message
//
// NOTE: Creating a stream does not block the storage from writing new messages can it is concurrent safe.
func (s *Storage) Stream(ctx context.Context, startPos int64) Stream {
	return &stream{
		id:       s.streamIdcount.Add(1),
		index:    -1,
		startPos: startPos,
		getFd:    s.getFd,
		putFd:    s.putFd,
		getLast:  s.getLastIndex,
		signal:   s.bc.CreateSignal(solid.WithHistory(startPos)),
	}
}

func (s *Storage) getLastIndex() int64 {
	return s.lastIndex
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
		_, err = w.Write(emptyFileHeader)
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

	// read the next 8 bytes to get the index of the last message

	var lastIndex int64
	err = binary.Read(w, binary.LittleEndian, &lastIndex)
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
		bc:        solid.NewBroadcast(solid.WithInitialTotal(count)),
		currSize:  size,
		currCount: count,
		lastIndex: lastIndex,
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
	id       int64
	index    int64
	startPos int64
	getFd    func(context.Context) (*os.File, error)
	putFd    func(context.Context, *os.File) error
	getLast  func() int64
	signal   *solid.Signal
}

func (s *stream) String() string {
	return fmt.Sprintf("stream-%d -> %d", s.id, s.index)
}

var _ Stream = (*stream)(nil)

func (s *stream) findIndex(fd *os.File) (int64, error) {
	// moved to the beginning of the file
	// to read the total number of messages

	fd.Seek(0, io.SeekStart)

	total, _, err := loadFileHeader(fd)
	if err != nil {
		return -1, err
	}

	// if startPos is negative, start from the latest messages

	if s.startPos < 0 || total-s.startPos <= 0 {
		return s.getLast(), nil
	} else if s.startPos == 0 {
		return FileHeaderSize, nil
	}

	var index int64 = FileHeaderSize

	for range s.startPos {
		var size int64
		err = binary.Read(fd, binary.LittleEndian, &size)
		if err != nil {
			return -1, err
		}

		_, err = fd.Seek(size, io.SeekCurrent)
		if err != nil {
			return -1, err
		}

		index += size + RecordHeaderSize
	}

	return index, nil
}

func (s *stream) Next(ctx context.Context) (r *Reader, size int64, err error) {
CHECK:
	err = s.signal.Wait(ctx)
	if err != nil {
		return nil, -1, err
	}

	fd, err := s.getFd(ctx)
	if err != nil {
		return nil, -1, err
	}

	defer func() {
		if err != nil {
			err1 := s.putFd(context.WithoutCancel(ctx), fd)
			if err1 != nil {
				err = errors.Join(err, fmt.Errorf("failed to put fd: %w", err1))
			}
		}
	}()

	if s.index == -1 {
		s.index, err = s.findIndex(fd)
		if err != nil {
			return nil, -1, err
		}
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

	return &Reader{
		r: io.LimitReader(fd, size),
		done: func() error {
			s.index += size + RecordHeaderSize
			return s.putFd(ctx, fd)
		},
	}, size, nil
}

func (s *stream) Done() {
	s.signal.Done()
}

//
// reader
//

type Reader struct {
	r    io.Reader
	done func() error
}

var _ io.Reader = (*Reader)(nil)

func (r *Reader) Read(p []byte) (n int, err error) {
	return r.r.Read(p)
}

func (r *Reader) Done() error {
	return r.done()
}
