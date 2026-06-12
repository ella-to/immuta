package immuta_test

import (
	"os"
	"testing"

	"ella.to/immuta"
)

func createStorage(t testing.TB, path string, namespaces ...string) (*immuta.Storage, func()) {
	return createStorageOpts(t, path, nil, namespaces...)
}

func createStorageOpts(t testing.TB, path string, opts []immuta.OptionFunc, namespaces ...string) (*immuta.Storage, func()) {
	os.RemoveAll(path)

	options := append([]immuta.OptionFunc{
		immuta.WithFastWrite(true),
		immuta.WithLogsDirPath(path),
		immuta.WithNamespaces(namespaces...),
	}, opts...)

	storage, err := immuta.New(options...)
	if err != nil {
		t.Fatalf("failed to create storage: %v", err)
	}
	return storage, func() {
		storage.Close()
		os.RemoveAll(path)
	}
}
