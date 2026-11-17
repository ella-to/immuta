package immuta

import (
	"os"
	"syscall"
)

// syncData flushes the file's data and the metadata needed to read it back
// (such as its size). fdatasync skips timestamps, which makes it cheaper
// than fsync for an append-only log.
func syncData(f *os.File) error {
	rc, err := f.SyscallConn()
	if err != nil {
		return f.Sync()
	}
	var syncErr error
	if err := rc.Control(func(fd uintptr) {
		for {
			syncErr = syscall.Fdatasync(int(fd))
			if syncErr != syscall.EINTR {
				return
			}
		}
	}); err != nil {
		return err
	}
	return syncErr
}
