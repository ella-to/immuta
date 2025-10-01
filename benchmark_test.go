package immuta_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"ella.to/immuta"
)

// Benchmark helpers
func createBenchmarkStorage(b *testing.B, path string) (*immuta.Storage, func()) {
	b.Helper()
	os.RemoveAll(path)

	storage, err := immuta.New(
		immuta.WithFastWrite(true),
		immuta.WithLogsDirPath(path),
		immuta.WithReaderCount(20), // Increase for concurrent benchmarks
		immuta.WithNamespaces("default"),
	)
	if err != nil {
		b.Fatalf("failed to create storage: %v", err)
	}
	return storage, func() {
		storage.Close()
		os.RemoveAll(path)
	}
}

// Sequential Write Benchmarks
func BenchmarkSequentialWrite_100B(b *testing.B) {
	storage, cleanup := createBenchmarkStorage(b, "./bench_seq_100b")
	defer cleanup()

	content := []byte(strings.Repeat("a", 100))
	b.ResetTimer()
	b.ReportAllocs()

	for range b.N {
		_, _, err := storage.Append(context.Background(), "default", bytes.NewReader(content))
		if err != nil {
			b.Fatal(err)
		}
	}

	// Report throughput
	b.ReportMetric(float64(b.N*100)/b.Elapsed().Seconds()/1024/1024, "MB/s")
}

func BenchmarkSequentialWrite_1KB(b *testing.B) {
	storage, cleanup := createBenchmarkStorage(b, "./bench_seq_1kb")
	defer cleanup()

	content := []byte(strings.Repeat("a", 1024))
	b.ResetTimer()
	b.ReportAllocs()

	for range b.N {
		_, _, err := storage.Append(context.Background(), "default", bytes.NewReader(content))
		if err != nil {
			b.Fatal(err)
		}
	}

	b.ReportMetric(float64(b.N*1024)/b.Elapsed().Seconds()/1024/1024, "MB/s")
}

func BenchmarkSequentialWrite_4KB(b *testing.B) {
	storage, cleanup := createBenchmarkStorage(b, "./bench_seq_4kb")
	defer cleanup()

	content := []byte(strings.Repeat("a", 4*1024))
	b.ResetTimer()
	b.ReportAllocs()

	for range b.N {
		_, _, err := storage.Append(context.Background(), "default", bytes.NewReader(content))
		if err != nil {
			b.Fatal(err)
		}
	}

	b.ReportMetric(float64(b.N*4*1024)/b.Elapsed().Seconds()/1024/1024, "MB/s")
}

func BenchmarkSequentialWrite_64KB(b *testing.B) {
	storage, cleanup := createBenchmarkStorage(b, "./bench_seq_64kb")
	defer cleanup()

	content := []byte(strings.Repeat("a", 64*1024))
	b.ResetTimer()
	b.ReportAllocs()

	for range b.N {
		_, _, err := storage.Append(context.Background(), "default", bytes.NewReader(content))
		if err != nil {
			b.Fatal(err)
		}
	}

	b.ReportMetric(float64(b.N*64*1024)/b.Elapsed().Seconds()/1024/1024, "MB/s")
}

// Concurrent Write Benchmarks
func BenchmarkConcurrentWrite_1KB_2Writers(b *testing.B) {
	benchmarkConcurrentWrite(b, 1024, 2)
}

func BenchmarkConcurrentWrite_1KB_4Writers(b *testing.B) {
	benchmarkConcurrentWrite(b, 1024, 4)
}

func BenchmarkConcurrentWrite_1KB_8Writers(b *testing.B) {
	benchmarkConcurrentWrite(b, 1024, 8)
}

func benchmarkConcurrentWrite(b *testing.B, msgSize, numWriters int) {
	storage, cleanup := createBenchmarkStorage(b, fmt.Sprintf("./bench_concurrent_%db_%dw", msgSize, numWriters))
	defer cleanup()

	content := []byte(strings.Repeat("a", msgSize))

	b.ResetTimer()
	b.ReportAllocs()

	var wg sync.WaitGroup
	writesPerWorker := b.N / numWriters

	for i := 0; i < numWriters; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < writesPerWorker; j++ {
				_, _, err := storage.Append(context.Background(), "default", bytes.NewReader(content))
				if err != nil {
					b.Error(err)
				}
			}
		}()
	}

	wg.Wait()

	totalBytes := float64(writesPerWorker * numWriters * msgSize)
	b.ReportMetric(totalBytes/b.Elapsed().Seconds()/1024/1024, "MB/s")
}

// Sequential Read Benchmarks
func BenchmarkSequentialRead_1KB(b *testing.B) {
	storage, cleanup := createBenchmarkStorage(b, "./bench_read_1kb")
	defer cleanup()

	// Pre-populate with data
	content := []byte(strings.Repeat("a", 1024))
	for range b.N {
		_, _, err := storage.Append(context.Background(), "default", bytes.NewReader(content))
		if err != nil {
			b.Fatal(err)
		}
	}

	stream := storage.Stream(context.Background(), "default", 0)
	defer stream.Done()

	b.ResetTimer()
	b.ReportAllocs()

	for range b.N {
		r, _, err := stream.Next(context.Background())
		if err != nil {
			b.Fatal(err)
		}
		_, err = io.Copy(io.Discard, r)
		if err != nil {
			b.Fatal(err)
		}
		r.Done()
	}

	b.ReportMetric(float64(b.N*1024)/b.Elapsed().Seconds()/1024/1024, "MB/s")
}

// Concurrent Read Benchmarks
func BenchmarkConcurrentRead_1KB_2Readers(b *testing.B) {
	benchmarkConcurrentRead(b, 1024, 2)
}

func BenchmarkConcurrentRead_1KB_4Readers(b *testing.B) {
	benchmarkConcurrentRead(b, 1024, 4)
}

func BenchmarkConcurrentRead_1KB_8Readers(b *testing.B) {
	benchmarkConcurrentRead(b, 1024, 8)
}

func benchmarkConcurrentRead(b *testing.B, msgSize, numReaders int) {
	storage, cleanup := createBenchmarkStorage(b, fmt.Sprintf("./bench_concurrent_read_%db_%dr", msgSize, numReaders))
	defer cleanup()

	// Pre-populate with data
	content := []byte(strings.Repeat("a", msgSize))
	numMessages := b.N
	for i := 0; i < numMessages; i++ {
		_, _, err := storage.Append(context.Background(), "default", bytes.NewReader(content))
		if err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	var wg sync.WaitGroup
	readsPerReader := numMessages / numReaders

	for i := 0; i < numReaders; i++ {
		wg.Add(1)
		go func(startPos int) {
			defer wg.Done()
			stream := storage.Stream(context.Background(), "default", int64(startPos*readsPerReader))
			defer stream.Done()

			for j := 0; j < readsPerReader; j++ {
				r, _, err := stream.Next(context.Background())
				if err != nil {
					b.Error(err)
					return
				}
				_, err = io.Copy(io.Discard, r)
				if err != nil {
					b.Error(err)
				}
				r.Done()
			}
		}(i)
	}

	wg.Wait()

	totalBytes := float64(readsPerReader * numReaders * msgSize)
	b.ReportMetric(totalBytes/b.Elapsed().Seconds()/1024/1024, "MB/s")
}

// Mixed Workload Benchmarks (Producer-Consumer)
func BenchmarkMixedWorkload_1Writer_2Readers(b *testing.B) {
	benchmarkMixedWorkload(b, 1024, 1, 2)
}

func BenchmarkMixedWorkload_2Writers_4Readers(b *testing.B) {
	benchmarkMixedWorkload(b, 1024, 2, 4)
}

func benchmarkMixedWorkload(b *testing.B, msgSize, numWriters, numReaders int) {
	storage, cleanup := createBenchmarkStorage(b, fmt.Sprintf("./bench_mixed_%db_%dw_%dr", msgSize, numWriters, numReaders))
	defer cleanup()

	content := []byte(strings.Repeat("a", msgSize))

	var writesCompleted, readsCompleted atomic.Int64
	var wg sync.WaitGroup

	// Total operations to perform
	totalOps := b.N
	if totalOps < 100 {
		totalOps = 100 // Ensure minimum operations for benchmark stability
	}

	b.ResetTimer()
	b.ReportAllocs()

	// Start writers
	writesPerWriter := totalOps / numWriters
	for i := 0; i < numWriters; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < writesPerWriter; j++ {
				_, _, err := storage.Append(context.Background(), "default", bytes.NewReader(content))
				if err != nil {
					b.Error(err)
					return
				}
				writesCompleted.Add(1)
			}
		}()
	}

	// Start readers after a small delay to ensure some data is written
	time.Sleep(10 * time.Millisecond)

	readsPerReader := totalOps / numReaders
	for i := 0; i < numReaders; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			stream := storage.Stream(context.Background(), "default", 0)
			defer stream.Done()

			for j := 0; j < readsPerReader; j++ {
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				r, _, err := stream.Next(ctx)
				cancel()
				if err != nil {
					if err == context.DeadlineExceeded {
						break // No more data to read within timeout
					}
					b.Error(err)
					return
				}
				_, err = io.Copy(io.Discard, r)
				if err != nil {
					b.Error(err)
				}
				r.Done()
				readsCompleted.Add(1)
			}
		}()
	}

	wg.Wait()

	writes := writesCompleted.Load()
	reads := readsCompleted.Load()
	totalBytes := float64((writes + reads) * int64(msgSize))
	b.ReportMetric(totalBytes/b.Elapsed().Seconds()/1024/1024, "MB/s")
	b.ReportMetric(float64(writes), "writes")
	b.ReportMetric(float64(reads), "reads")
}

// Variable Size Message Benchmarks
func BenchmarkVariableSizeWrite(b *testing.B) {
	storage, cleanup := createBenchmarkStorage(b, "./bench_var_size")
	defer cleanup()

	// Create messages of varying sizes (100B to 10KB)
	rng := rand.New(rand.NewSource(42))
	messages := make([][]byte, 1000)
	for i := range messages {
		size := 100 + rng.Intn(10*1024-100) // 100B to 10KB
		messages[i] = make([]byte, size)
		for j := range messages[i] {
			messages[i][j] = byte('a' + (j % 26))
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	var totalBytes int64
	for i := range b.N {
		msg := messages[i%len(messages)]
		totalBytes += int64(len(msg))
		_, _, err := storage.Append(context.Background(), "default", bytes.NewReader(msg))
		if err != nil {
			b.Fatal(err)
		}
	}

	b.ReportMetric(float64(totalBytes)/b.Elapsed().Seconds()/1024/1024, "MB/s")
}

// Memory Usage and GC Pressure Benchmarks
func BenchmarkMemoryPressure_1KB_10K(b *testing.B) {
	storage, cleanup := createBenchmarkStorage(b, "./bench_memory")
	defer cleanup()

	content := []byte(strings.Repeat("a", 1024))

	var m1, m2 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m1)

	b.ResetTimer()

	for i := 0; i < 10000; i++ {
		_, _, err := storage.Append(context.Background(), "default", bytes.NewReader(content))
		if err != nil {
			b.Fatal(err)
		}
	}

	runtime.GC()
	runtime.ReadMemStats(&m2)

	b.ReportMetric(float64(m2.TotalAlloc-m1.TotalAlloc)/1024/1024, "MB_allocated")
	b.ReportMetric(float64(m2.NumGC-m1.NumGC), "GC_runs")
	b.ReportMetric(float64(m2.Alloc)/1024/1024, "MB_heap")
}

// Latency Benchmarks (measure single operation latency)
func BenchmarkWriteLatency_1KB(b *testing.B) {
	storage, cleanup := createBenchmarkStorage(b, "./bench_latency")
	defer cleanup()

	content := []byte(strings.Repeat("a", 1024))

	b.ResetTimer()

	for range b.N {
		start := time.Now()
		_, _, err := storage.Append(context.Background(), "default", bytes.NewReader(content))
		if err != nil {
			b.Fatal(err)
		}
		latency := time.Since(start)
		b.ReportMetric(float64(latency.Nanoseconds()), "ns/op")
	}
}

// High-throughput stress test
func BenchmarkHighThroughputStress(b *testing.B) {
	storage, cleanup := createBenchmarkStorage(b, "./bench_stress")
	defer cleanup()

	content := []byte(strings.Repeat("a", 1024))
	numWorkers := runtime.NumCPU()

	b.ResetTimer()
	b.ReportAllocs()

	var wg sync.WaitGroup
	writesPerWorker := b.N / numWorkers

	start := time.Now()

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < writesPerWorker; j++ {
				_, _, err := storage.Append(context.Background(), "default", bytes.NewReader(content))
				if err != nil {
					b.Error(err)
				}
			}
		}()
	}

	wg.Wait()

	elapsed := time.Since(start)
	totalBytes := float64(writesPerWorker * numWorkers * 1024)

	b.ReportMetric(totalBytes/elapsed.Seconds()/1024/1024, "MB/s")
	b.ReportMetric(float64(writesPerWorker*numWorkers)/elapsed.Seconds(), "ops/s")
}
