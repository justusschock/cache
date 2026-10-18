package disk_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	cache "github.com/justusschock/cache"
	"github.com/justusschock/cache/backend/disk"
)

func ExampleStore_ScanPrefix() {
	dir, err := os.MkdirTemp("", "go-cache-example")
	if err != nil {
		panic(err)
	}
	defer func() { _ = os.RemoveAll(dir) }()

	store, err := disk.Open(filepath.Join(dir, "hashes.db"), disk.WithOrderedKeys[string, string]())
	if err != nil {
		panic(err)
	}
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	paths := []string{"src/b.go", "docs/readme.md", "src/a.go"}
	entries := []cache.Entry[string]{
		{Value: "hash-b", Found: true},
		{Value: "hash-readme", Found: true},
		{Value: "hash-a", Found: true},
	}
	if err := store.SetMany(ctx, "file-hashes", paths, entries); err != nil {
		panic(err)
	}

	err = store.ScanPrefix(ctx, "file-hashes", []byte("src/"), func(path string, entry cache.Entry[string]) bool {
		fmt.Println(path, entry.Value)
		return true
	})
	if err != nil {
		panic(err)
	}
	removed, err := store.DeletePrefix(ctx, "file-hashes", []byte("src/"))
	if err != nil {
		panic(err)
	}
	fmt.Println("removed", removed)
	// Output:
	// src/a.go hash-a
	// src/b.go hash-b
	// removed 2
}
