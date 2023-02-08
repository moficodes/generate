package main

import (
	"context"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"os"
	"runtime"
	"time"

	"golang.org/x/sync/errgroup"
)

var count int
var goroutine int
var filename string
var bufferSize int

var dataPerGoroutine int

func init() {
	rand.Seed(time.Now().UnixNano())
	flag.IntVar(&count, "count", 0, "number of record to generate")
	flag.IntVar(&goroutine, "goroutine", 0, "number of goroutine to run")
	flag.StringVar(&filename, "file", "input.txt", "name of the file")
	flag.IntVar(&bufferSize, "buffer", 1, "buffer size in Mb")

	flag.Parse()
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

func generateNumber() int {
	return rand.Intn(math.MaxInt64)
}

func generateNumberHex(number int) []byte {
	return []byte(fmt.Sprintf("%019x", number))
}

func writeToFile(filename string, goroutines, dataPerGoroutine int, ctx context.Context) error {
	errs, _ := errgroup.WithContext(ctx)
	for i := 0; i < goroutines; i++ {
		errs.Go(func() error {
			f, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
			if err != nil {
				return err
			}

			var buf []byte
			buf = make([]byte, 0, bufferSize*1024*1024)
			for j := 0; j < dataPerGoroutine; j++ {
				data := generateNumberHex(generateNumber())
				buf = append(buf, data...)
				buf = append(buf, '\n')
				if len(buf)+32 > cap(buf) {
					if _, err := f.Write(buf); err != nil {
						return err
					}
					buf = make([]byte, 0, bufferSize*1024*1024)
				}
			}

			if len(buf) > 0 {
				if _, err := f.Write(buf); err != nil {
					return err
				}
			}

			if err := f.Close(); err != nil {
				return err
			}
			return nil
		})
	}

	return errs.Wait()
}

func main() {
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
	writeToFile(filename, goroutine, dataPerGoroutine, context.Background())
	PrintMemUsage()
	fmt.Println()
}
