package cache_test

import (
	"context"
	"fmt"

	cache "github.com/justusschock/go-cache"
	"github.com/justusschock/go-cache/backend/memory"
)

func ExampleNoExpiry() {
	store := memory.New[string, string]()
	defer func() { _ = store.Close() }()

	hashes, err := cache.New("file-hashes", cache.NoExpiry, store)
	if err != nil {
		panic(err)
	}
	ctx := context.Background()
	if err := hashes.Set(ctx, "src/main.go", "sha256:ab12"); err != nil {
		panic(err)
	}
	entry, present, err := hashes.GetEntry(ctx, "src/main.go")
	if err != nil {
		panic(err)
	}
	fmt.Println(present, entry.Value, entry.NeverExpires())
	// Output: true sha256:ab12 true
}

func ExampleStandardCache_GetMany() {
	store := memory.New[string, int]()
	defer func() { _ = store.Close() }()

	sizes, err := cache.New("file-sizes", cache.NoExpiry, store)
	if err != nil {
		panic(err)
	}
	ctx := context.Background()
	if err := sizes.SetMany(ctx, []string{"a.txt", "b.txt"}, []int{10, 20}); err != nil {
		panic(err)
	}
	values, found, err := sizes.GetMany(ctx, []string{"b.txt", "missing.txt", "a.txt"})
	if err != nil {
		panic(err)
	}
	fmt.Println(values, found)
	// Output: [20 0 10] [true false true]
}
