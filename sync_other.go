//go:build !linux

package immuta

import "os"

// syncData flushes the file to stable storage. On macOS os.File.Sync uses
// F_FULLFSYNC, which is required for durability there.
func syncData(f *os.File) error {
	return f.Sync()
}
