package cachetest

import "fmt"

// Go runs fn in a goroutine and reports returned errors or recovered panics.
func Go(results chan<- error, fn func() error) {
	go func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				results <- fmt.Errorf("goroutine panicked: %v", recovered)
			}
		}()
		results <- fn()
	}()
}
