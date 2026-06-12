package immuta_test

import (
	"bytes"
	"fmt"
	"sync"
	"testing"

	"ella.to/immuta"
)

func BenchmarkAppend_100B(b *testing.B) { benchmarkAppend(b, 100) }
func BenchmarkAppend_1KB(b *testing.B)  { benchmarkAppend(b, 1024) }
func BenchmarkAppend_4KB(b *testing.B)  { benchmarkAppend(b, 4*1024) }
func BenchmarkAppend_64KB(b *testing.B) { benchmarkAppend(b, 64*1024) }

func benchmarkAppend(b *testing.B, size int) {
	namespace := "bench"
	storage, cleanup := createStorage(b, fmt.Sprintf("./BenchmarkAppend_%d", size), namespace)
	defer cleanup()

	payload := bytes.Repeat([]byte{'a'}, size)
	b.SetBytes(int64(size))
	b.ReportAllocs()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, err := storage.Append(b.Context(), namespace, bytes.NewReader(payload))
		storage.Save(namespace, &err)
		if err != nil {
			b.Fatalf("append failed: %v", err)
		}
	}
}

// BenchmarkAppendBatch100_1KB commits 100 appends per Save, the intended
// usage pattern for high write throughput (group commit).
func BenchmarkAppendBatch100_1KB(b *testing.B) {
	namespace := "bench"
	storage, cleanup := createStorage(b, "./BenchmarkAppendBatch100_1KB", namespace)
	defer cleanup()

	payload := bytes.Repeat([]byte{'a'}, 1024)
	b.SetBytes(1024)
	b.ReportAllocs()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var err error
		if _, _, err = storage.Append(b.Context(), namespace, bytes.NewReader(payload)); err != nil {
			b.Fatalf("append failed: %v", err)
		}
		if i%100 == 99 {
			storage.Save(namespace, &err)
			if err != nil {
				b.Fatalf("save failed: %v", err)
			}
		}
	}
	var err error
	storage.Save(namespace, &err)
	if err != nil {
		b.Fatalf("final save failed: %v", err)
	}
}

// BenchmarkAppendUnknownSize_1KB exercises the placeholder-header path used
// when the source does not expose Len() (e.g. transformed/streamed content).
func BenchmarkAppendUnknownSize_1KB(b *testing.B) {
	namespace := "bench"
	storage, cleanup := createStorage(b, "./BenchmarkAppendUnknownSize_1KB", namespace)
	defer cleanup()

	payload := bytes.Repeat([]byte{'a'}, 1024)
	b.SetBytes(1024)
	b.ReportAllocs()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, err := storage.Append(b.Context(), namespace, hideLen{bytes.NewReader(payload)})
		storage.Save(namespace, &err)
		if err != nil {
			b.Fatalf("append failed: %v", err)
		}
	}
}

// BenchmarkAppendDurable_1KB opens the log with O_SYNC (fastWrite=false):
// every write syscall is flushed to stable storage.
func BenchmarkAppendDurable_1KB(b *testing.B) {
	namespace := "bench"
	storage, cleanup := createStorageOpts(b, "./BenchmarkAppendDurable_1KB",
		[]immuta.OptionFunc{immuta.WithFastWrite(false)}, namespace)
	defer cleanup()

	payload := bytes.Repeat([]byte{'a'}, 1024)
	b.SetBytes(1024)
	b.ReportAllocs()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, err := storage.Append(b.Context(), namespace, bytes.NewReader(payload))
		storage.Save(namespace, &err)
		if err != nil {
			b.Fatalf("append failed: %v", err)
		}
	}
}

// BenchmarkAppendSyncOnSaveBatch100_1KB is the recommended durable mode on
// SSDs: buffered writes with one fsync per 100-message commit.
func BenchmarkAppendSyncOnSaveBatch100_1KB(b *testing.B) {
	namespace := "bench"
	storage, cleanup := createStorageOpts(b, "./BenchmarkAppendSyncOnSaveBatch100_1KB",
		[]immuta.OptionFunc{immuta.WithSyncOnSave(true)}, namespace)
	defer cleanup()

	payload := bytes.Repeat([]byte{'a'}, 1024)
	b.SetBytes(1024)
	b.ReportAllocs()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var err error
		if _, _, err = storage.Append(b.Context(), namespace, bytes.NewReader(payload)); err != nil {
			b.Fatalf("append failed: %v", err)
		}
		if i%100 == 99 {
			storage.Save(namespace, &err)
			if err != nil {
				b.Fatalf("save failed: %v", err)
			}
		}
	}
	var err error
	storage.Save(namespace, &err)
	if err != nil {
		b.Fatalf("final save failed: %v", err)
	}
}

func BenchmarkRead_100B(b *testing.B) { benchmarkRead(b, 100) }
func BenchmarkRead_1KB(b *testing.B)  { benchmarkRead(b, 1024) }
func BenchmarkRead_64KB(b *testing.B) { benchmarkRead(b, 64*1024) }

func benchmarkRead(b *testing.B, size int) {
	namespace := "bench"
	storage, cleanup := createStorage(b, fmt.Sprintf("./BenchmarkRead_%d", size), namespace)
	defer cleanup()

	payload := bytes.Repeat([]byte{'a'}, size)

	b.ReportAllocs()
	b.SetBytes(int64(size))

	// Pre-fill exactly b.N records so the benchmark measures read throughput.
	b.StopTimer()
	for i := 0; i < b.N; i++ {
		_, _, err := storage.Append(b.Context(), namespace, bytes.NewReader(payload))
		storage.Save(namespace, &err)
		if err != nil {
			b.Fatalf("prefill append failed: %v", err)
		}
	}

	stream := storage.Stream(b.Context(), namespace, 0)
	defer stream.Done()
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		r, _, err := stream.Next(b.Context())
		if err != nil {
			b.Fatalf("read failed: %v", err)
		}
		r.Done()
	}
}

// BenchmarkReadFull_1KB also consumes the payload, measuring end-to-end read
// throughput rather than just record iteration.
func BenchmarkReadFull_1KB(b *testing.B) {
	namespace := "bench"
	storage, cleanup := createStorage(b, "./BenchmarkReadFull_1KB", namespace)
	defer cleanup()

	payload := bytes.Repeat([]byte{'a'}, 1024)
	buf := make([]byte, 1024)

	b.ReportAllocs()
	b.SetBytes(1024)

	b.StopTimer()
	for i := 0; i < b.N; i++ {
		_, _, err := storage.Append(b.Context(), namespace, bytes.NewReader(payload))
		storage.Save(namespace, &err)
		if err != nil {
			b.Fatalf("prefill append failed: %v", err)
		}
	}

	stream := storage.Stream(b.Context(), namespace, 0)
	defer stream.Done()
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		r, size, err := stream.Next(b.Context())
		if err != nil {
			b.Fatalf("read failed: %v", err)
		}
		if _, err := r.Read(buf[:size]); err != nil {
			b.Fatalf("payload read failed: %v", err)
		}
		r.Done()
	}
}

// BenchmarkStreamConcurrent8_1KB runs 8 independent streams reading the same
// b.N records concurrently, stressing the shared fd pool.
func BenchmarkStreamConcurrent8_1KB(b *testing.B) {
	const readers = 8
	namespace := "bench"
	storage, cleanup := createStorageOpts(b, "./BenchmarkStreamConcurrent8_1KB",
		[]immuta.OptionFunc{immuta.WithReaderCount(readers)}, namespace)
	defer cleanup()

	payload := bytes.Repeat([]byte{'a'}, 1024)

	b.ReportAllocs()
	b.SetBytes(readers * 1024)

	b.StopTimer()
	for i := 0; i < b.N; i++ {
		_, _, err := storage.Append(b.Context(), namespace, bytes.NewReader(payload))
		storage.Save(namespace, &err)
		if err != nil {
			b.Fatalf("prefill append failed: %v", err)
		}
	}

	streams := make([]immuta.Stream, readers)
	for i := range streams {
		streams[i] = storage.Stream(b.Context(), namespace, 0)
		defer streams[i].Done()
	}
	b.StartTimer()

	var wg sync.WaitGroup
	wg.Add(readers)
	for i := 0; i < readers; i++ {
		go func(stream immuta.Stream) {
			defer wg.Done()
			for j := 0; j < b.N; j++ {
				r, _, err := stream.Next(b.Context())
				if err != nil {
					b.Errorf("read failed: %v", err)
					return
				}
				r.Done()
			}
		}(streams[i])
	}
	wg.Wait()
}

// BenchmarkStreamSeek100K measures positioning a new stream deep inside a
// log of 100,000 small records (the findIndex header scan).
func BenchmarkStreamSeek100K(b *testing.B) {
	const total = 100_000
	namespace := "bench"
	storage, cleanup := createStorage(b, "./BenchmarkStreamSeek100K", namespace)
	defer cleanup()

	payload := bytes.Repeat([]byte{'a'}, 100)
	for i := 0; i < total; i++ {
		var err error
		if _, _, err = storage.Append(b.Context(), namespace, bytes.NewReader(payload)); err != nil {
			b.Fatalf("prefill append failed: %v", err)
		}
		if i%1000 == 999 {
			storage.Save(namespace, &err)
			if err != nil {
				b.Fatalf("prefill save failed: %v", err)
			}
		}
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		stream := storage.Stream(b.Context(), namespace, total-1)
		r, _, err := stream.Next(b.Context())
		if err != nil {
			b.Fatalf("read failed: %v", err)
		}
		r.Done()
		stream.Done()
	}
}
