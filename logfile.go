package immuta

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"ella.to/solid"
)

const (
	FileHeaderSize   = 8 + 8 // total number of messages + index of the last message
	RecordHeaderSize = 8     // size of the content of the message

	// smallRecordThreshold is the largest payload that is staged in memory and
	// written together with its record header in a single write syscall.
	smallRecordThreshold = 64 * 1024

	// scanChunkSize is the read-ahead window used when walking record headers
	// (findIndex, Verify). One pread covers many small records.
	scanChunkSize = 512 * 1024
)

var (
	emptyFileHeader   = make([]byte, FileHeaderSize)
	emptyRecordHeader = make([]byte, RecordHeaderSize)
)

// scanBufPool recycles the read-ahead windows used by skipRecords, so
// creating a stream at a deep start position does not allocate every time.
var scanBufPool = sync.Pool{
	New: func() any {
		b := make([]byte, scanChunkSize)
		return &b
	},
}

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
	w      *os.File
	fds    chan *os.File
	allFds []*os.File
	bc     solid.Broadcast
	// wbuf stages small records (header + payload) so they are written with a
	// single syscall. Append is single-writer, so no locking is needed.
	wbuf      []byte
	currSize  int64
	currCount int64
	lastIndex int64
	// transaction pending info for deferred Save
	txPending      bool
	txIndex        int64
	txPendingCount int64 // number of pending appends not yet committed
	txBaseSize     int64 // file size before first pending append
	streamIdcount  atomic.Int64
	closed         atomic.Bool
	syncOnSave     bool        // fsync data and header during Save (group commit)
	writeTransform Transformer // transforms data before writing
	readTransform  Transformer // transforms data after reading
}

var _ Appender = (*storage)(nil)

func (s *storage) getFd(ctx context.Context) (*os.File, error) {
	select {
	case fd := <-s.fds:
		return fd, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// putFd returns a reader fd to the pool. The channel has capacity for every
// reader fd and is never closed, so this never blocks and never panics even
// when racing with Close. Close closes the underlying files directly; an fd
// returned after that simply yields ErrClosed on the next read.
func (s *storage) putFd(fd *os.File) {
	s.fds <- fd
}

func loadFileHeader(fd *os.File) (total int64, index int64, err error) {
	var hdr [FileHeaderSize]byte
	if _, err = fd.ReadAt(hdr[:], 0); err != nil {
		return -1, -1, err
	}
	total = int64(binary.LittleEndian.Uint64(hdr[:8]))
	index = int64(binary.LittleEndian.Uint64(hdr[8:]))
	return total, index, nil
}

// skipRecords walks n record headers starting at FileHeaderSize and returns
// the offset just past the n-th record along with the offset of the n-th
// record itself. Headers are read through a chunked pread cache so skipping
// many small records costs one syscall per scanChunkSize window instead of
// two syscalls per record.
func skipRecords(fd *os.File, n int64) (end int64, last int64, err error) {
	bufp := scanBufPool.Get().(*[]byte)
	defer scanBufPool.Put(bufp)
	buf := *bufp
	var (
		off        int64 = FileHeaderSize
		chunkStart int64 = -1
		chunkLen   int64
	)
	last = -1
	for range n {
		if chunkStart < 0 || off < chunkStart || off+RecordHeaderSize > chunkStart+chunkLen {
			m, rerr := fd.ReadAt(buf, off)
			if m < RecordHeaderSize {
				if rerr == nil || errors.Is(rerr, io.EOF) {
					rerr = io.ErrUnexpectedEOF
				}
				return -1, -1, rerr
			}
			chunkStart, chunkLen = off, int64(m)
		}
		size := int64(binary.LittleEndian.Uint64(buf[off-chunkStart:]))
		last = off
		off += RecordHeaderSize + size
	}
	return off, last, nil
}

func (s *storage) Details(ctx context.Context) (string, error) {
	if s.closed.Load() {
		return "", ErrStorageClosed
	}

	fd, err := s.getFd(ctx)
	if err != nil {
		return "", err
	}
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

	fd, err := s.getFd(ctx)
	if err != nil {
		return err
	}
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

	calculateSize, currIndex, err := skipRecords(fd, total)
	if err != nil {
		return err
	}
	if total == 0 {
		currIndex = 0
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
//
// The writer fd offset is kept at the end of the file at all times (Save
// patches the file header with pwrite), so appends are purely sequential
// writes with no seeks. If the source reader exposes its length via
// Len() int (bytes.Reader, bytes.Buffer, strings.Reader, ...) and the
// payload is small, the record header and payload are written with a single
// syscall.
func (s *storage) Append(ctx context.Context, r io.Reader) (index int64, size int64, err error) {
	if s.closed.Load() {
		return -1, -1, ErrStorageClosed
	}

	// If this is the first append in a pending transaction, record the base size
	if !s.txPending {
		s.txBaseSize = s.currSize
	}

	index = s.currSize

	var contentReader io.Reader = r
	knownLen := -1
	if s.writeTransform != nil {
		// a transform may change the content size, so it is always unknown
		contentReader, err = s.writeTransform(r)
		if err != nil {
			return -1, -1, fmt.Errorf("failed to transform content: %w", err)
		}
	} else if l, ok := r.(interface{ Len() int }); ok {
		knownLen = l.Len()
	}

	switch {
	case knownLen >= 0 && knownLen <= smallRecordThreshold:
		// stage header + payload and write the whole record in one syscall
		need := knownLen + RecordHeaderSize
		if cap(s.wbuf) < need {
			s.wbuf = make([]byte, smallRecordThreshold+RecordHeaderSize)
		}
		buf := s.wbuf[:need]
		binary.LittleEndian.PutUint64(buf[:RecordHeaderSize], uint64(knownLen))
		if _, err = io.ReadFull(contentReader, buf[RecordHeaderSize:]); err != nil {
			// nothing has been written yet, the file is untouched
			return -1, -1, fmt.Errorf("failed to read content: %w", err)
		}
		if _, err = s.w.Write(buf); err != nil {
			s.discardPartialWrite(index)
			return -1, -1, err
		}
		size = int64(knownLen)

	case knownLen > smallRecordThreshold:
		// the size is known up front, so the real header can be written
		// first and the payload streamed after it; no patching needed
		var hdr [RecordHeaderSize]byte
		binary.LittleEndian.PutUint64(hdr[:], uint64(knownLen))
		if _, err = s.w.Write(hdr[:]); err != nil {
			s.discardPartialWrite(index)
			return -1, -1, err
		}
		size, err = s.copyContent(contentReader)
		if err == nil && size != int64(knownLen) {
			err = fmt.Errorf("content size changed during append: expected %d, copied %d", knownLen, size)
		}
		if err != nil {
			s.discardPartialWrite(index)
			return -1, -1, err
		}

	default:
		// unknown size: write a placeholder header, stream the content, then
		// patch the header in place with pwrite (does not move the fd offset)
		if _, err = s.w.Write(emptyRecordHeader); err != nil {
			s.discardPartialWrite(index)
			return -1, -1, err
		}
		size, err = s.copyContent(contentReader)
		if err != nil {
			s.discardPartialWrite(index)
			return -1, -1, err
		}
		var hdr [RecordHeaderSize]byte
		binary.LittleEndian.PutUint64(hdr[:], uint64(size))
		if _, err = s.w.WriteAt(hdr[:], index); err != nil {
			s.discardPartialWrite(index)
			return -1, -1, err
		}
	}

	// set pending transaction info; caller should call Save(&err) (deferred) to commit/rollback
	s.txPending = true
	s.txIndex = index
	s.txPendingCount++

	// advance in-memory current size so subsequent appends append after this
	s.currSize = index + size + RecordHeaderSize

	return index, size, nil
}

// copyContent streams r to the log file. Sources that implement io.WriterTo
// (bytes.Reader, bytes.Buffer, strings.Reader, ...) write themselves out
// directly; everything else is copied through the reused staging buffer so no
// transient copy buffer is allocated per append (io.Copy would allocate 32KB
// for every call).
func (s *storage) copyContent(r io.Reader) (written int64, err error) {
	if _, ok := r.(io.WriterTo); ok {
		return io.Copy(s.w, r)
	}

	if cap(s.wbuf) == 0 {
		s.wbuf = make([]byte, smallRecordThreshold+RecordHeaderSize)
	}
	buf := s.wbuf[:cap(s.wbuf)]
	for {
		n, rerr := r.Read(buf)
		if n > 0 {
			wn, werr := s.w.Write(buf[:n])
			written += int64(wn)
			if werr != nil {
				return written, werr
			}
			if wn < n {
				return written, io.ErrShortWrite
			}
		}
		if rerr != nil {
			if errors.Is(rerr, io.EOF) {
				return written, nil
			}
			return written, rerr
		}
	}
}

// discardPartialWrite removes any bytes a failed Append left behind and
// restores the writer fd offset, so the file stays consistent even if the
// caller never invokes Save or commits other pending appends afterwards.
func (s *storage) discardPartialWrite(offset int64) {
	_ = s.w.Truncate(offset)
	_, _ = s.w.Seek(offset, io.SeekStart)
}

func (s *storage) Close() error {
	// Check if already closed
	if s.closed.Swap(true) {
		return nil // Already closed
	}

	// Close the broadcast first to wake up all waiting streams
	s.bc.Close()

	// Close every reader fd directly. The fds channel itself is never closed,
	// so a concurrent putFd cannot panic; in-flight readers observe ErrClosed.
	var errs []error
	for _, fd := range s.allFds {
		if err := fd.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	if err := s.w.Close(); err != nil {
		errs = append(errs, err)
	}

	return errors.Join(errs...)
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
		s.rollback(err)
		return
	}

	// Make the appended data durable before the header publishes it, so a
	// crash cannot leave the header pointing at unwritten records.
	if s.syncOnSave {
		if syncErr := s.w.Sync(); syncErr != nil {
			s.abortCommit(err, fmt.Errorf("failed to sync appended data: %w", syncErr))
			return
		}
	}

	// Commit: patch the file header in place (pwrite keeps the writer fd
	// offset at the end of the file, so the next Append needs no seek)
	currCount := s.currCount + s.txPendingCount
	var header [FileHeaderSize]byte
	binary.LittleEndian.PutUint64(header[:8], uint64(currCount))
	binary.LittleEndian.PutUint64(header[8:], uint64(s.txIndex))

	if _, writeErr := s.w.WriteAt(header[:], 0); writeErr != nil {
		s.abortCommit(err, fmt.Errorf("failed to write the header: %w", writeErr))
		return
	}

	if s.syncOnSave {
		if syncErr := s.w.Sync(); syncErr != nil {
			s.abortCommit(err, fmt.Errorf("failed to sync the header: %w", syncErr))
			return
		}
	}

	// update counters (currSize already reflects appended data)
	s.lastIndex = s.txIndex
	s.currCount = currCount

	// notify waiting readers of the number of new messages
	s.bc.Notify(s.txPendingCount)
}

// rollback truncates uncommitted appends and restores in-memory state.
func (s *storage) rollback(err *error) {
	if truncErr := s.w.Truncate(s.txBaseSize); truncErr != nil {
		if err != nil {
			*err = errors.Join(*err, fmt.Errorf("failed to truncate the file: %w", truncErr))
		}
	}
	if _, seekErr := s.w.Seek(s.txBaseSize, io.SeekStart); seekErr != nil {
		if err != nil {
			*err = errors.Join(*err, fmt.Errorf("failed to restore write offset: %w", seekErr))
		}
	}
	s.currSize = s.txBaseSize
}

// abortCommit handles a failure inside the commit phase of Save: it restores
// the previous header (best effort), discards the pending appends and reports
// the failure, so the file is not left with an orphaned uncounted tail.
func (s *storage) abortCommit(err *error, cause error) {
	var header [FileHeaderSize]byte
	binary.LittleEndian.PutUint64(header[:8], uint64(s.currCount))
	binary.LittleEndian.PutUint64(header[8:], uint64(s.lastIndex))
	_, _ = s.w.WriteAt(header[:], 0)

	if err != nil {
		*err = cause
	}
	s.rollback(err)
}

// Stream(ctx, 0) 	-> from the beginning
// Stream(ctx, -1) 	-> start from latest messages
// Stream(ctx, 10) 	-> start after 10nth message
//
// NOTE: Creating a stream does not block the storage from writing new messages and it is concurrent safe.
func (s *storage) Stream(ctx context.Context, startPos int64) Stream {
	return &stream{
		id:       s.streamIdcount.Add(1),
		index:    -1,
		startPos: startPos,
		st:       s,
		signal:   s.bc.CreateSignal(solid.WithHistory(startPos)),
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
	syncOnSave     bool
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

// WithSyncOnSave makes Save fsync the file before and after publishing the
// header, so committed messages survive a crash or power loss. Combined with
// WithFastWrite(true) this is the recommended durability mode on modern
// SSDs: any number of appends are batched and the cost of flushing is paid
// once per Save (group commit) instead of once per write syscall (O_SYNC).
func WithSyncOnSave(enabled bool) OptionFunc {
	return func(o *options) error {
		o.syncOnSave = enabled
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
		st, err := newStorage(logFile, o)
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

func newStorage(filepath string, o *options) (*storage, error) {
	if o.readerCount <= 0 {
		return nil, ErrReaderCountIsZero
	}

	var flag int

	if o.fastWrite {
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
		size = FileHeaderSize
	}

	count, lastIndex, err := loadFileHeader(w)
	if err != nil {
		w.Close()
		return nil, err
	}

	// position the write cursor at the end of the file; Append and Save keep
	// it there from now on (the header is patched with pwrite)
	_, err = w.Seek(size, io.SeekStart)
	if err != nil {
		w.Close()
		return nil, err
	}

	st := &storage{
		w:              w,
		fds:            make(chan *os.File, o.readerCount),
		allFds:         make([]*os.File, 0, o.readerCount),
		bc:             solid.NewBroadcastCond(solid.WithInitialTotal(count)),
		currSize:       size,
		currCount:      count,
		lastIndex:      lastIndex,
		syncOnSave:     o.syncOnSave,
		writeTransform: o.writeTransform,
		readTransform:  o.readTransform,
	}

	for range o.readerCount {
		fd, err := os.Open(filepath)
		if err != nil {
			for _, opened := range st.allFds {
				opened.Close()
			}
			w.Close()
			return nil, err
		}
		st.allFds = append(st.allFds, fd)
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
	id       int64
	index    int64
	startPos int64
	st       *storage
	signal   solid.Signal
}

func (s *stream) String() string {
	return fmt.Sprintf("stream-%d -> %d", s.id, s.index)
}

var _ Stream = (*stream)(nil)

func (s *stream) findIndex(fd *os.File) (int64, error) {
	total, _, err := loadFileHeader(fd)
	if err != nil {
		return -1, err
	}

	// if startPos is negative, start from the latest messages

	if s.startPos < 0 || total-s.startPos <= 0 {
		return s.st.getLastIndex(), nil
	} else if s.startPos == 0 {
		return FileHeaderSize, nil
	}

	index, _, err := skipRecords(fd, s.startPos)
	return index, err
}

func (s *stream) Next(ctx context.Context) (*Reader, int64, error) {
	for {
		// Check if storage is closed before waiting
		if s.st.closed.Load() {
			return nil, -1, ErrStorageClosed
		}

		if s.signal == nil {
			// the broadcast was already closed when this stream was created
			return nil, -1, ErrStorageClosed
		}

		// Wait for signal WITHOUT holding an FD - this allows unlimited concurrent streams
		err := s.signal.Wait(ctx)
		if err != nil {
			// Check if the error is because broadcast is closed
			if errors.Is(err, solid.ErrSignalNotAvailable) {
				return nil, -1, ErrStorageClosed
			}
			return nil, -1, err
		}

		// Check if storage is closed after waking up from wait
		if s.st.closed.Load() {
			return nil, -1, ErrStorageClosed
		}

		// NOW acquire FD only when we need to read
		fd, err := s.st.getFd(ctx)
		if err != nil {
			return nil, -1, err
		}

		// Initialize index on first read
		if s.index == -1 {
			s.index, err = s.findIndex(fd)
			if err != nil {
				s.st.putFd(fd)
				return nil, -1, err
			}
		}

		// Read the size of the next message with pread; no seek is needed and
		// the fd's offset is never used, so reads cannot interfere
		var hdr [RecordHeaderSize]byte
		_, err = fd.ReadAt(hdr[:], s.index)
		if errors.Is(err, io.EOF) {
			// No data available yet, release FD and wait for next signal
			s.st.putFd(fd)
			continue
		} else if err != nil {
			s.st.putFd(fd)
			return nil, -1, fmt.Errorf("failed to read size of content at %d, id: %d: %w", s.index, s.id, err)
		}
		size := int64(binary.LittleEndian.Uint64(hdr[:]))

		// Success - return Reader that will release FD when done
		return &Reader{
			fd:            fd,
			off:           s.index + RecordHeaderSize,
			remaining:     size,
			size:          size,
			stream:        s,
			readTransform: s.st.readTransform,
		}, size, nil
	}
}

func (s *stream) Done() {
	if s.signal != nil {
		s.signal.Done()
	}
}

//
// reader
//

// Reader reads the payload of a single record directly from the underlying
// file with positional reads (pread).
type Reader struct {
	fd            *os.File
	off           int64
	remaining     int64
	size          int64
	stream        *stream
	readTransform Transformer
	transformed   io.Reader
	transformErr  error
	finished      bool
}

var _ io.Reader = (*Reader)(nil)

// rawReader exposes the untransformed record bytes as an io.Reader for a
// read transformer, without an extra allocation.
type rawReader Reader

func (r *rawReader) Read(p []byte) (int, error) {
	return (*Reader)(r).readRaw(p)
}

func (r *Reader) readRaw(p []byte) (int, error) {
	if r.remaining <= 0 {
		return 0, io.EOF
	}
	if int64(len(p)) > r.remaining {
		p = p[:r.remaining]
	}
	n, err := r.fd.ReadAt(p, r.off)
	r.off += int64(n)
	r.remaining -= int64(n)
	if errors.Is(err, io.EOF) {
		if r.remaining > 0 {
			// the record claims more bytes than the file holds
			return n, io.ErrUnexpectedEOF
		}
		// the record ends exactly at the end of the file; report success now
		// and io.EOF on the next call
		err = nil
	}
	return n, err
}

func (r *Reader) Read(p []byte) (n int, err error) {
	if r.readTransform == nil {
		return r.readRaw(p)
	}

	// If transform is enabled and we haven't transformed yet, do it now
	if r.transformed == nil && r.transformErr == nil {
		r.transformed, r.transformErr = r.readTransform((*rawReader)(r))
	}

	// If we had an error during transformation, return it
	if r.transformErr != nil {
		return 0, r.transformErr
	}

	return r.transformed.Read(p)
}

func (r *Reader) Done() {
	if r.finished {
		return
	}
	r.finished = true
	r.stream.index += r.size + RecordHeaderSize
	r.stream.st.putFd(r.fd)
	r.fd = nil
}
