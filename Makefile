all:
	go run .

part1:
	go run . -mode part1

part2:
	go run . -mode part2

build:
	go build -o client.exe

master:
	go run . -master -wait data/austen.db  out.db

clean:
	rm -rf tmp/* *.db client.exe