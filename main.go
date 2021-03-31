package main

import (
	"log"
)

func main() {
	log.SetFlags(log.Lshortfile)

	_, err := splitDatabase("data/austen.db", "tmp", "output-%d.db", 100)
	if err != nil {
		log.Fatalf("split db: %v", err)
	}

	// for _, p := range paths {
	// 	fmt.Println(p)
	// }
}
