BINARY := filesplit

clean:
	rm -f $(BINARY)
	rm -f input_*.txt

build: clean
	CGO_ENABLED=0 go build -ldflags="-s -w" .

run: build
	./$(BINARY) -h
