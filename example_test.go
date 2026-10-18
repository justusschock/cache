package cache_test

import (
	"context"
	"fmt"
	"time"

	cache "github.com/justusschock/go-cache"
	"github.com/justusschock/go-cache/backend/memory"
)

type User struct {
	ID   string
	Name string
}

func Example() {
	store := memory.New[string, User]()
	defer func() { _ = store.Close() }()

	users, err := cache.New("users", 5*time.Minute, store)
	if err != nil {
		panic(err)
	}

	ctx := context.Background()
	if err := users.Set(ctx, "user-42", User{ID: "user-42", Name: "Ada"}); err != nil {
		panic(err)
	}
	user, found, err := users.Get(ctx, "user-42")
	if err != nil {
		panic(err)
	}
	fmt.Println(found, user.Name)
	// Output: true Ada
}

func ExampleNewPullThrough() {
	store := memory.New[string, User]()
	defer func() { _ = store.Close() }()
	users, err := cache.New("users", 5*time.Minute, store)
	if err != nil {
		panic(err)
	}

	loads := 0
	loaded, err := cache.NewPullThrough(
		"users",
		users,
		func(_ context.Context, id string) (User, bool, error) {
			loads++
			if id != "user-42" {
				return User{}, false, nil // authoritative miss, cached negatively
			}
			return User{ID: id, Name: "Ada"}, true, nil
		},
		cache.WithNegativeTTL[string](30*time.Second),
	)
	if err != nil {
		panic(err)
	}

	ctx := context.Background()
	for range 2 {
		user, found, err := loaded.Get(ctx, "user-42")
		if err != nil {
			panic(err)
		}
		fmt.Println(found, user.Name)
	}
	_, found, err := loaded.Get(ctx, "user-7")
	if err != nil {
		panic(err)
	}
	fmt.Println(found, "loads:", loads)
	// Output:
	// true Ada
	// true Ada
	// false loads: 2
}

func ExampleNewLayered() {
	fast := memory.New[string, User]()
	defer func() { _ = fast.Close() }()
	shared := memory.New[string, User]()
	defer func() { _ = shared.Close() }()

	layers, err := cache.NewLayered([]cache.Store[string, User]{fast, shared})
	if err != nil {
		panic(err)
	}
	ctx := context.Background()
	entry := cache.Entry[User]{Value: User{ID: "user-42", Name: "Ada"}, Found: true}
	if err := shared.Set(ctx, "users", "user-42", entry); err != nil {
		panic(err)
	}

	// A lower-layer hit is promoted into the faster layer.
	if _, _, err := layers.Get(ctx, "users", "user-42"); err != nil {
		panic(err)
	}
	promoted, present, err := fast.Get(ctx, "users", "user-42")
	if err != nil {
		panic(err)
	}
	fmt.Println(present, promoted.Value.Name)
	// Output: true Ada
}

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
