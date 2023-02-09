package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"math"
	"math/rand"
	"os"
	"runtime"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
)

var count int
var goroutine int
var filename string
var bufferSize int

var dataPerGoroutine int

var r = rand.New(rand.NewSource(time.Now().UnixNano()))

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

func generateNumber() int {
	return r.Intn(math.MaxInt)
}

func generateNumberHex(number int) []byte {
	return []byte(fmt.Sprintf("%019x", number))
}

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
	for i := 0; i < goroutines; i++ {
		errs.Go(func() error {
			buf := make([]byte, bufferSize*1024*1024, bufferSize*1024*1024)
			index := 0
			for j := 0; j < dataPerGoroutine; j++ {
				data := generateNumberHex(generateNumber())

				for k := 0; k < len(data); k++ {
					buf[index] = data[k]
					index++
				}
				buf[index] = '\n'
				index++

				if index+20 > len(buf) {
					filelock.Lock()
					if _, err := w.Write(buf[:index]); err != nil {
						filelock.Unlock()
						return err
					}
					filelock.Unlock()
					buf = make([]byte, bufferSize*1024*1024, bufferSize*1024*1024)
					index = 0
				}
			}

			if index > 0 {
				filelock.Lock()
				if _, err := w.Write(buf[:index]); err != nil {
					filelock.Unlock()
					return err
				}
				filelock.Unlock()
			}
			return nil
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
