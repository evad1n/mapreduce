all:
	go run .

part1:
	go run . -mode part1

part2:
	go run . -mode part2

build:
	go build -o client

master:
	go run . -master data/austen.db  out.db

clean:
	rm -rf tmp/* *.db