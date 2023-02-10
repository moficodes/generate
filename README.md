# Generate

This project is for generating a lot of numbers.

## Usage

```bash
go build
./generate -h
```

Should show you the help message.

```bash
./generate -h
Usage of ./generate:
  -buffer int
        buffer size in Mb (default 1)
  -count int
        number of record to generate
  -file string
        name of the file (default "input.txt")
  -goroutine int
        number of goroutine to run
  -linelength int
        length of the line (length of each number + 1 for newline) (default 17)
  -version
        print version and exit
```

## Example

```bash
# will generate 1000000 numbers in input.txt with 1Mb buffer and 5 goroutine
./generate -count 1000000 -file input.txt -buffer 1 -goroutine 5
```
