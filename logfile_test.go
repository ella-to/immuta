package immuta_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"

	"ella.to/immuta"
)

func TestFileAppendLog(t *testing.T) {
	f, err := immuta.New(immuta.WithFastWrite("./TestFileAppendLog.log"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		f.Close()
		os.Remove("./TestFileAppendLog.log")
	})

	content := []byte("hello world")
	length := len(content)

	for i := 0; i < 10; i++ {
		size, err := f.Append(bytes.NewReader(content))
		if err != nil {
			t.Fatal(err)
		}

		if size != int64(length) {
			t.Fatalf("size = %d, want 11", size)
		}
	}

	reader, err := f.Stream(context.TODO())
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Done()

	for i := 0; i < 10; i++ {
		r, size, err := reader.Next()
		if err != nil {
			t.Fatal(err)
		}

		if size != int64(length) {
			t.Fatalf("size = %d, want 11", size)
		}

		buf := new(bytes.Buffer)
		buf.ReadFrom(r)
		if !bytes.Equal(buf.Bytes(), content) {
			t.Fatalf("content = %s, want hello world", buf.String())
		}
	}

	_, _, err = reader.Next()
	if err != io.EOF {
		t.Fatalf("err = %v, want EOF", err)
	}
}

func TestCheckData(t *testing.T) {
	f, err := immuta.New(immuta.WithFastWrite("./TestCheckData.log"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		f.Close()
		os.Remove("./TestCheckData.log")
	})

	content := []byte("hello world")

	_, err = f.Append(bytes.NewReader(content))
	if err != nil {
		t.Fatal(err)
	}

	reader, err := f.Stream(context.TODO())
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Done()

	r, size, err := reader.Next()
	if err != nil {
		t.Fatal(err)
	}

	if size != int64(len(content)) {
		t.Fatalf("size = %d, want 11", size)
	}

	buf := new(bytes.Buffer)
	buf.ReadFrom(r)
	if !bytes.Equal(buf.Bytes(), content) {
		t.Fatalf("content = %s, want hello world", buf.String())
	}

	_, _, err = reader.Next()
	if err != io.EOF {
		t.Fatalf("err = %v, want EOF", err)
	}

	_, err = f.Append(bytes.NewReader(content))
	if err != nil {
		t.Fatal(err)
	}

	_, _, err = reader.Next()
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
}

func TestWithRelativeMessage(t *testing.T) {
	f, err := immuta.New(immuta.WithFastWrite("./TestWithRelativeMessage.log"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		f.Close()
		os.Remove("./TestWithRelativeMessage.log")
	})

	for i := range 10 {
		_, err := f.Append(strings.NewReader(fmt.Sprintf("hello world %d", i+1)))
		if err != nil {
			t.Fatal(err)
		}
	}

	reader, err := f.Stream(context.TODO(), immuta.WithRelativeMessage(6))
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Done()

	r, size, err := reader.Next()
	if err != nil {
		t.Fatal(err)
	}

	// The first message is "hello world 7"
	if size != 13 {
		t.Fatalf("size = %d, want 13", size)
	}

	buf := new(bytes.Buffer)

	buf.ReadFrom(r)

	if !bytes.Equal(buf.Bytes(), []byte("hello world 7")) {
		t.Fatalf("content = %s, want hello world 7", buf.String())
	}
}
