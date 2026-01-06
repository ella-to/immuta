package immuta_test

import (
	"bytes"
	"fmt"
	"testing"
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

func BenchmarkRead_1KB(b *testing.B) {
	namespace := "bench"
	storage, cleanup := createStorage(b, "./BenchmarkRead_1KB", namespace)
	defer cleanup()

	payload := bytes.Repeat([]byte{'a'}, 1024)

	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))

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
