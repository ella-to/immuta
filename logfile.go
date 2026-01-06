package immuta

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
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

var (
	ErrNamespaceRequired = errors.New("namespace is required")
	ErrNamesapceNotFound = errors.New("namespace not found")
	ErrStorageClosed     = errors.New("storage is closed")
	ErrReaderCountIsZero = errors.New("reader count must be greater than zero")
)

// Transformer transforms a reader into another reader.
// This can be used for compression, encryption, or any other data transformation.
// Multiple transformers can be chained together.
type Transformer func(r io.Reader) (io.Reader, error)

// Appender is a single method interface that writes the content of the reader to the storage medium.
type Appender interface {
	// Append writes the content of the reader to the storage medium.
	// and returns the index and size of the content written.
	Append(ctx context.Context, r io.Reader) (index int64, size int64, err error)
	// Save implements the transactional commit/rollback for the most recent Append.
	// Should be deferred by the caller as: defer storage.Save(&err)
	Save(err *error)
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

type storage struct {
	w         *os.File
	fds       chan *os.File
	bc        solid.Broadcast
	currSize  int64
	currCount int64
	lastIndex int64
	// transaction pending info for deferred Save
	txPending      bool
	txIndex        int64
	txSize         int64
	txPendingCount int64 // number of pending appends not yet committed
	txBaseSize     int64 // file size before first pending append
	streamIdcount  atomic.Int64
	closed         atomic.Bool
	writeTransform Transformer // transforms data before writing
	readTransform  Transformer // transforms data after reading
}

var _ Appender = (*storage)(nil)

func (s *storage) getFd(ctx context.Context) *os.File {
	select {
	case fd := <-s.fds:
		return fd
	case <-ctx.Done():
		return nil
	}
}

func (s *storage) putFd(fd *os.File) {
	if s.closed.Load() {
		fd.Close()
		return
	}
	s.fds <- fd
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

func (s *storage) Details(ctx context.Context) (string, error) {
	if s.closed.Load() {
		return "", ErrStorageClosed
	}

	fd := s.getFd(ctx)
	defer s.putFd(fd)

	stat, err := fd.Stat()
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("size: %d, count: %d", stat.Size(), s.currCount), nil
}

func (s *storage) Verify(ctx context.Context) error {
	if s.closed.Load() {
		return ErrStorageClosed
	}

	fd := s.getFd(ctx)
	defer s.putFd(fd)

	stat, err := fd.Stat()
	if err != nil {
		return err
	}

	size := stat.Size()

	total, lastIndex, err := loadFileHeader(fd)
	if err != nil {
		return err
	}

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
func (s *storage) Append(ctx context.Context, r io.Reader) (index int64, size int64, err error) {
	if s.closed.Load() {
		return -1, -1, ErrStorageClosed
	}

	// If this is the first append in a pending transaction, record the base size
	if !s.txPending {
		s.txBaseSize = s.currSize
	}

	index = s.currSize

	_, err = s.w.Seek(s.currSize, io.SeekStart)
	if err != nil {
		return -1, -1, err
	}

	// write header first (placeholder)
	_, err = s.w.Write(emptyRecordHeader)
	if err != nil {
		return -1, -1, err
	}

	// Apply write transform if provided
	var contentReader io.Reader = r
	if s.writeTransform != nil {
		contentReader, err = s.writeTransform(r)
		if err != nil {
			return -1, -1, fmt.Errorf("failed to transform content: %w", err)
		}
	}

	// copy the content
	size, err = io.Copy(s.w, contentReader)
	if err != nil {
		// leave file as-is; caller's deferred Save is expected to rollback
		return -1, -1, err
	}

	currSize := s.currSize + size + RecordHeaderSize

	// Move back to the header of the content
	_, err = s.w.Seek(currSize-(size+RecordHeaderSize), io.SeekStart)
	if err != nil {
		return -1, -1, err
	}

	// write the size of the content
	err = binary.Write(s.w, binary.LittleEndian, size)
	if err != nil {
		return -1, -1, err
	}

	// set pending transaction info; caller should call Save(&err) (deferred) to commit/rollback
	s.txPending = true
	s.txIndex = index
	s.txSize = size
	s.txPendingCount++

	// advance in-memory current size so subsequent appends append after this
	s.currSize = currSize

	return index, size, nil
}

func (s *storage) Close() error {
	// Check if already closed
	if s.closed.Swap(true) {
		return nil // Already closed
	}

	// Close the broadcast first to wake up all waiting streams
	s.bc.Close()

	// Close all file descriptor readers
	close(s.fds)
	for fd := range s.fds {
		if err := fd.Close(); err != nil {
			return err
		}
	}

	return s.w.Close()
}

// Save performs a transactional commit or rollback for the last Append.
// It is intended to be deferred by callers as: defer st.Save(&err)
func (s *storage) Save(err *error) {
	// If there is no pending tx, nothing to do
	if !s.txPending {
		return
	}
	// Ensure transactional flags are cleared when done
	defer func() {
		s.txPending = false
		s.txPendingCount = 0
	}()

	// If caller signaled an error, rollback by truncating the file to the base size
	if err != nil && *err != nil {
		truncErr := s.w.Truncate(s.txBaseSize)
		if truncErr != nil {
			*err = errors.Join(*err, fmt.Errorf("failed to truncate the file: %w", truncErr))
		}
		// restore in-memory state
		s.currSize = s.txBaseSize
		return
	}

	// Commit: update the file header and in-memory counters
	_, seekErr := s.w.Seek(0, io.SeekStart)
	if seekErr != nil {
		if err != nil {
			*err = fmt.Errorf("failed to seek to the beginning of the file: %w", seekErr)
		}
		return
	}

	currCount := s.currCount + s.txPendingCount
	var header [16]byte
	binary.LittleEndian.PutUint64(header[:8], uint64(currCount))
	binary.LittleEndian.PutUint64(header[8:], uint64(s.txIndex))

	_, writeErr := s.w.Write(header[:])
	if writeErr != nil {
		if err != nil {
			*err = fmt.Errorf("failed to write the header: %w", writeErr)
		}
		return
	}

	// update counters (currSize already reflects appended data)
	s.lastIndex = s.txIndex
	s.currCount = currCount

	// notify waiting readers of the number of new messages
	s.bc.Notify(s.txPendingCount)
}

// Stream(ctx, 0) 	-> from the beginning
// Stream(ctx, -1) 	-> start from latest messages
// Stream(ctx, 10) 	-> start after 10nth message
//
// NOTE: Creating a stream does not block the storage from writing new messages and it is concurrent safe.
func (s *storage) Stream(ctx context.Context, startPos int64) Stream {
	return &stream{
		id:            s.streamIdcount.Add(1),
		index:         -1,
		startPos:      startPos,
		getFd:         s.getFd,
		putFd:         s.putFd,
		getLast:       s.getLastIndex,
		isClosed:      func() bool { return s.closed.Load() },
		signal:        s.bc.CreateSignal(solid.WithHistory(startPos)),
		readTransform: s.readTransform,
	}
}

func (s *storage) getLastIndex() int64 {
	return s.lastIndex
}

type Storage struct {
	mapper map[string]*storage
	closed atomic.Bool
}

func (s *Storage) Close() error {
	if s.closed.Swap(true) {
		return nil // Already closed
	}
	var errs []error
	for _, st := range s.mapper {
		if err := st.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

func (s *Storage) getStorageByNamespace(namespace string) (*storage, error) {
	if namespace == "" {
		return nil, ErrNamespaceRequired
	}

	st, ok := s.mapper[namespace]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrNamesapceNotFound, namespace)
	}
	return st, nil
}

func (s *Storage) Append(ctx context.Context, namespace string, r io.Reader) (index int64, size int64, err error) {
	st, err := s.getStorageByNamespace(namespace)
	if err != nil {
		return -1, -1, err
	}

	return st.Append(ctx, r)
}

// Save performs a transactional commit/rollback for the most recent Append
// on the specified namespace. Intended to be used as: defer s.Save(namespace, &err)
func (s *Storage) Save(namespace string, err *error) {
	st, gerr := s.getStorageByNamespace(namespace)
	if gerr != nil {
		if err != nil && *err == nil {
			*err = gerr
		}
		return
	}
	st.Save(err)
}

func (s *Storage) Stream(ctx context.Context, namespace string, startPos int64) Stream {
	st, err := s.getStorageByNamespace(namespace)
	if err != nil {
		panic(err)
	}

	return st.Stream(ctx, startPos)
}

func (s *Storage) Details(ctx context.Context, namespace string) (string, error) {
	st, err := s.getStorageByNamespace(namespace)
	if err != nil {
		return "", err
	}

	return st.Details(ctx)
}

func (s *Storage) Verify(ctx context.Context, namespace string) error {
	st, err := s.getStorageByNamespace(namespace)
	if err != nil {
		return err
	}

	return st.Verify(ctx)
}

type options struct {
	logsDirPath    string
	readerCount    int
	fastWrite      bool
	namespaces     []string
	writeTransform Transformer
	readTransform  Transformer
}

type OptionFunc func(*options) error

func WithLogsDirPath(path string) OptionFunc {
	return func(o *options) error {
		o.logsDirPath = path
		return nil
	}
}

func WithReaderCount(count int) OptionFunc {
	return func(o *options) error {
		if count <= 0 {
			return ErrReaderCountIsZero
		}

		o.readerCount = count
		return nil
	}
}

func WithFastWrite(fast bool) OptionFunc {
	return func(o *options) error {
		o.fastWrite = fast
		return nil
	}
}

func WithNamespaces(namespaces ...string) OptionFunc {
	return func(o *options) error {
		o.namespaces = namespaces
		return nil
	}
}

// WithWriteTransform sets a transformer to be applied when writing data.
// This can be used for compression, encryption, etc.
func WithWriteTransform(t Transformer) OptionFunc {
	return func(o *options) error {
		o.writeTransform = t
		return nil
	}
}

// WithReadTransform sets a transformer to be applied when reading data.
// This can be used for decompression, decryption, etc.
func WithReadTransform(t Transformer) OptionFunc {
	return func(o *options) error {
		o.readTransform = t
		return nil
	}
}

// ChainTransformers chains multiple transformers together.
// Transformers are applied in order.
func ChainTransformers(transformers ...Transformer) Transformer {
	return func(r io.Reader) (io.Reader, error) {
		var err error
		for _, t := range transformers {
			if t == nil {
				continue
			}
			r, err = t(r)
			if err != nil {
				return nil, err
			}
		}
		return r, nil
	}
}

func New(optFns ...OptionFunc) (*Storage, error) {
	o := &options{
		logsDirPath: "./logs",
		readerCount: 5,
		fastWrite:   true,
		namespaces:  nil,
	}

	for _, fn := range optFns {
		if err := fn(o); err != nil {
			return nil, err
		}
	}

	if len(o.namespaces) == 0 {
		return nil, ErrNamespaceRequired
	}

	if o.readerCount <= 0 {
		return nil, ErrReaderCountIsZero
	}

	err := os.MkdirAll(o.logsDirPath, 0o755)
	if err != nil && !errors.Is(err, os.ErrExist) {
		return nil, fmt.Errorf("failed to create logs directory %s: %w", o.logsDirPath, err)
	}

	mapper := make(map[string]*storage, len(o.namespaces))
	for _, namespace := range o.namespaces {
		logFile := filepath.Join(o.logsDirPath, fmt.Sprintf("%s.log", namespace))
		st, err := newStorage(logFile, o.readerCount, o.fastWrite, o.writeTransform, o.readTransform)
		if err != nil {
			// Close any already opened storages
			for _, opened := range mapper {
				opened.Close()
			}
			return nil, err
		}
		mapper[namespace] = st
	}

	return &Storage{
		mapper: mapper,
	}, nil
}

func newStorage(filepath string, readerCount int, fastWrite bool, writeTransform, readTransform Transformer) (*storage, error) {
	if readerCount <= 0 {
		return nil, ErrReaderCountIsZero
	}

	var flag int

	if fastWrite {
		flag = os.O_RDWR | os.O_CREATE
	} else {
		flag = os.O_RDWR | os.O_CREATE | os.O_SYNC
	}

	w, err := os.OpenFile(filepath, flag, 0o644)
	if err != nil {
		return nil, err
	}

	size, err := getSize(w)
	if err != nil {
		w.Close()
		return nil, err
	}

	if size == 0 {
		_, err = w.Write(emptyFileHeader)
		if err != nil {
			w.Close()
			return nil, err
		}
	}

	_, err = w.Seek(0, io.SeekStart)
	if err != nil {
		w.Close()
		return nil, err
	}

	// read the first 8 bytes to get the number of messages
	var count int64
	err = binary.Read(w, binary.LittleEndian, &count)
	if err != nil {
		w.Close()
		return nil, err
	}

	// read the next 8 bytes to get the index of the last message
	var lastIndex int64
	err = binary.Read(w, binary.LittleEndian, &lastIndex)
	if err != nil {
		w.Close()
		return nil, err
	}

	// move the cursor to the end of the file
	size, err = getSize(w)
	if err != nil {
		w.Close()
		return nil, err
	}

	_, err = w.Seek(size, io.SeekStart)
	if err != nil {
		w.Close()
		return nil, err
	}

	st := &storage{
		w:              w,
		fds:            make(chan *os.File, readerCount),
		bc:             solid.NewBroadcastCond(solid.WithInitialTotal(count)),
		currSize:       size,
		currCount:      count,
		lastIndex:      lastIndex,
		writeTransform: writeTransform,
		readTransform:  readTransform,
	}

	for range readerCount {
		fd, err := os.Open(filepath)
		if err != nil {
			return nil, err
		}
		st.fds <- fd
	}

	return st, nil
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
	id            int64
	index         int64
	startPos      int64
	getFd         func(ctx context.Context) *os.File
	putFd         func(*os.File)
	getLast       func() int64
	isClosed      func() bool
	signal        solid.Signal
	readTransform Transformer
}

func (s *stream) String() string {
	return fmt.Sprintf("stream-%d -> %d", s.id, s.index)
}

var _ Stream = (*stream)(nil)

func (s *stream) findIndex(fd *os.File) (int64, error) {
	// moved to the beginning of the file
	// to read the total number of messages

	_, err := fd.Seek(0, io.SeekStart)
	if err != nil {
		return -1, err
	}

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
	for {
		// Check if storage is closed before waiting
		if s.isClosed() {
			return nil, -1, ErrStorageClosed
		}

		// Wait for signal WITHOUT holding an FD - this allows unlimited concurrent streams
		err = s.signal.Wait(ctx)
		if err != nil {
			// Check if the error is because broadcast is closed
			if errors.Is(err, solid.ErrSignalNotAvailable) {
				return nil, -1, ErrStorageClosed
			}
			return nil, -1, err
		}

		// Check if storage is closed after waking up from wait
		if s.isClosed() {
			return nil, -1, ErrStorageClosed
		}

		// NOW acquire FD only when we need to read
		fd := s.getFd(ctx)
		if fd == nil {
			return nil, -1, ctx.Err()
		}

		// Initialize index on first read
		if s.index == -1 {
			s.index, err = s.findIndex(fd)
			if err != nil {
				s.putFd(fd)
				return nil, -1, err
			}
		}

		// Seek to current position
		_, err = fd.Seek(s.index, io.SeekStart)
		if err != nil {
			s.putFd(fd)
			return nil, -1, fmt.Errorf("failed to seek to index %d, id: %d: %w", s.index, s.id, err)
		}

		// Read the size of the next message
		err = binary.Read(fd, binary.LittleEndian, &size)
		if errors.Is(err, io.EOF) {
			// No data available yet, release FD and wait for next signal
			s.putFd(fd)
			continue
		} else if err != nil {
			s.putFd(fd)
			return nil, -1, fmt.Errorf("failed to read size of content at %d, id: %d: %w", s.index, s.id, err)
		}

		// Success - return Reader that will release FD when done
		return &Reader{
			r:             io.LimitReader(fd, size),
			readTransform: s.readTransform,
			done: func() {
				s.index += size + RecordHeaderSize
				s.putFd(fd)
			},
		}, size, nil
	}
}

func (s *stream) Done() {
	s.signal.Done()
}

//
// reader
//

type Reader struct {
	r             io.Reader
	readTransform Transformer
	transformed   io.Reader
	done          func()
	transformErr  error
}

var _ io.Reader = (*Reader)(nil)

func (r *Reader) Read(p []byte) (n int, err error) {
	// If transform is enabled and we haven't transformed yet, do it now
	if r.readTransform != nil && r.transformed == nil && r.transformErr == nil {
		r.transformed, r.transformErr = r.readTransform(r.r)
	}

	// If we had an error during transformation, return it
	if r.transformErr != nil {
		return 0, r.transformErr
	}

	// Read from transformed reader if available, otherwise from raw reader
	if r.transformed != nil {
		return r.transformed.Read(p)
	}

	return r.r.Read(p)
}

func (r *Reader) Done() {
	r.done()
}
