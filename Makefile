all:
	go run .

master:
	go run . -master data/austen.db  out.db

clean:
	rm -rf tmp/* *.db