package cache_test

import "fmt"

// goReport runs fn in a goroutine and sends its returned error, or an error
// describing a recovered panic, to results.
func goReport(results chan<- error, fn func() error) {
	go func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				results <- fmt.Errorf("goroutine panicked: %v", recovered)
			}
		}()
		results <- fn()
	}()
}
