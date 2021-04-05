all: clean
	go run .

clean:
	rm -rf tmp/* out.db