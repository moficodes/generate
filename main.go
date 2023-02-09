package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"os"
	"runtime"
	"strconv"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
)

var count int
var goroutine int
var filename string
var bufferSize int

var dataPerGoroutine int

func init() {
	flag.IntVar(&count, "count", 0, "number of record to generate")
	flag.IntVar(&goroutine, "goroutine", 0, "number of goroutine to run")
	flag.StringVar(&filename, "file", "input.txt", "name of the file")
	flag.IntVar(&bufferSize, "buffer", 1, "buffer size in Mb")
}

// PrintMemUsage outputs the current, total and OS memory being used. As well as the number
// of garage collection cycles completed.
func PrintMemUsage() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	// For info on each, see: https://golang.org/pkg/runtime/#MemStats
	fmt.Printf("alloc = %v MiB", bToMb(m.Alloc))
	fmt.Printf("\ttotalAlloc = %v MiB", bToMb(m.TotalAlloc))
	fmt.Printf("\tsys = %v MiB", bToMb(m.Sys))
	fmt.Printf("\tnumGC = %v\n", m.NumGC)
}

func duration(msg string, start time.Time) {
	fmt.Printf("%s took %s\n", msg, time.Since(start))
}

func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}

func generateNumber(r *rand.Rand) int64 {
	return r.Int63()
}

func generateNumberHex(number int64) []byte {
	return []byte(fmt.Sprintf("%019x", number))
}

// appendNumberHex is like generateNumberHex, but more efficient and with a more difficult API
// When b has sufficient capacity, no allocation is involved.
func appendNumberHex(number int64, b []byte) []byte {
	return fmt.Appendf(b, "%019x", number)
}

// appendInt16 is like generateNumberHex, but more efficient and with a more difficult API
// When b has sufficient capacity, no allocation is involved.
func appendInt16(number int64, b []byte) []byte {
	// return strconv.AppendInt(b, number, 16)
	// strconv.AppendInt is fast but doesn't do the padding for us

	var scratch [256]byte // this is on the stack (probably)
	data := strconv.AppendInt(scratch[:0], number, 16)

	p := 19 - len(data) // number of '0' to be padded
	if p > 0 {
		b = append(b, zeroes[:p]...)
	}
	return append(b, data...)
}

// zeroes is only read by appendInt16, never modified
var zeroes = bytes.Repeat([]byte{'0'}, 19)

func writeToFile(ctx context.Context, filename string, goroutines, dataPerGoroutine int) error {
	f, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	return write(ctx, f, goroutines, dataPerGoroutine)
}

func write(ctx context.Context, w io.Writer, goroutines, dataPerGoroutine int) error {
	errs, _ := errgroup.WithContext(ctx)
	var filelock sync.Mutex
	bufferByteSize := bufferSize * 1024 * 1024

	for i := 0; i < goroutines; i++ {
		errs.Go(func() error {
			r := rand.New(rand.NewSource(time.Now().UnixNano()))
			buf := make([]byte, 0, bufferByteSize)

			flushAndReuse := func() error {
				// Flush to w
				filelock.Lock()
				defer filelock.Unlock()
				_, err := w.Write(buf)
				// Reset and reuse
				buf = buf[:0]
				return err
			}

			for j := 0; j < dataPerGoroutine; j++ {
				buf = appendInt16(generateNumber(r), buf)
				buf = append(buf, '\n')

				if len(buf)+256 > len(buf) {
					if err := flushAndReuse(); err != nil {
						return err
					}
				}
			}

			return flushAndReuse()
		})
	}

	return errs.Wait()
}

func main() {
	flag.Parse()

	if goroutine > count {
		goroutine = count
	}

	if goroutine == 0 {
		goroutine = runtime.GOMAXPROCS(-1)
	}

	if count == 0 {
		fmt.Println("no data to produce")
		os.Exit(1)
	}

	dataPerGoroutine = count / goroutine
	count = count - (count % goroutine)

	fmt.Printf("total count: %d\ngoroutine: %d\n", count, goroutine)
	fmt.Printf("gen per goroutine: %d\n", dataPerGoroutine)
	fmt.Printf("total gen: %d\n", bToMb(uint64(count*20)))

	fmt.Println()

	defer duration("gen number", time.Now())
	writeToFile(context.Background(), filename, goroutine, dataPerGoroutine)
	PrintMemUsage()
	fmt.Println()
}
