package immuta_test

import (
	"os"
	"testing"

	"ella.to/immuta"
)

func createStorage(t testing.TB, path string, namespaces ...string) (*immuta.Storage, func()) {
	os.RemoveAll(path)

	storage, err := immuta.New(
		immuta.WithFastWrite(true),
		immuta.WithLogsDirPath(path),
		immuta.WithNamespaces(namespaces...),
	)
	if err != nil {
		t.Fatalf("failed to create storage: %v", err)
	}
	return storage, func() {
		storage.Close()
		os.RemoveAll(path)
	}
}
