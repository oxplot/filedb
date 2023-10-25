# FileDB

FileDB is a simple file-based database written in Go. It provides basic CRUD operations on documents, which are stored as JSON files in a directory structure. The library also handles concurrent modifications and provides a simple key-based access to the documents.

## Installation

To use FileDB in your Go project, you can install it by running:

```bash
go get github.com/oxplot/filedb
```

## Usage

Here is a basic example of how to use FileDB:

```go
package main

import (
	"fmt"
	"github.com/yourusername/filedb"
)

func main() {
	db, err := filedb.Open("/path/to/db")
	if err != nil {
		panic(err)
	}

	err = db.Set("key", "value")
	if err != nil {
		panic(err)
	}

	value, err := db.Get("key")
	if err != nil {
		panic(err)
	}

	fmt.Println(value) // Output: value
}
```
