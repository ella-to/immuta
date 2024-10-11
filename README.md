```
██╗███╗░░░███╗███╗░░░███╗██╗░░░██╗████████╗░█████╗░
██║████╗░████║████╗░████║██║░░░██║╚══██╔══╝██╔══██╗
██║██╔████╔██║██╔████╔██║██║░░░██║░░░██║░░░███████║
██║██║╚██╔╝██║██║╚██╔╝██║██║░░░██║░░░██║░░░██╔══██║
██║██║░╚═╝░██║██║░╚═╝░██║╚██████╔╝░░░██║░░░██║░░██║
╚═╝╚═╝░░░░░╚═╝╚═╝░░░░░╚═╝░╚═════╝░░░░╚═╝░░░╚═╝░░╚═╝
```

Immuta is a `Append Only Log` implementation based on single writer, multiple readers concept. It uses filesystem as it's core the format of the each record is as follows

- the first 8 bytes define the size of the payload (Header)
- payload can be any arbitrary size
- records are one after another

```
+----------+---------------+----------+---------------+
|          |               |          |               |
|  HEADER  |    PAYLOAD    |  HEADER  |    PAYLOAD    | ...
|          |               |          |               |
+----------+---------------+----------+---------------+
```

# Installation

```bash
go get ella.to/immuta
```

# Usgae

```golang

import (
    "bytes"
    "io"
    "fmt"

    "ella.to/immuta"
)


func main() {
    log, err := immuta.New(immuta.WithFastWrite("./my.log"))
    if err != nil {
        panic(err)
    }
    defer log.Close()

    content := []byte("hello world")

    // write to append only log
    size, err := log.Append(bytes.NewReader(content))
    if err != nil {
        panic(err)
    }

    if size != 11 {
        panic("size must be 11")
    }

    // for reading, create a stream
    reader, err := log.Stream(context.TODO(), 0)
    if err != nil {
        panic(err)
    }
    // make sure to signal immuta that you have done reading
    // so the resoucre can be reused again
    defer reader.Done()

    for {
        r, size, err := reader.Next()
        if errors.Is(err, io.EOF) {
            break
        }

        data, err := io.ReadAll(r)
        if err != nil {
            panic(err)
        }

        // you should see on screen
        // Data is: hello world, with size of 11
        fmt.Printf("Data is: %s, with size of %d", string(data), size)
    }
}
```
