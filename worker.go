package main

type (
	Interface interface {
		Map(key, value string, output chan<- Pair) error
		Reduce(key string, values <-chan string, output chan<- Pair) error
	}

	Pair struct {
		Key   string
		Value string
	}
)
