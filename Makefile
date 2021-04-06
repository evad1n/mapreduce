all:
	go run .

build:
	go build -o client

master:
	go run . -master data/austen.db  out.db

clean:
	rm -rf tmp/* *.db