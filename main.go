package main

import (
	"bufio"
	"context"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"os"
	"runtime"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
)

var (
	count      int
	goroutine  int
	filename   string
	bufferSize int
	version    bool
	linelength int
	VERSION    string = "v0.0.0"
)

var dataPerGoroutine int

func init() {
	flag.IntVar(&count, "count", 0, "number of record to generate")
	flag.IntVar(&goroutine, "goroutine", 0, "number of goroutine to run")
	flag.StringVar(&filename, "file", "input.txt", "name of the file")
	flag.IntVar(&bufferSize, "buffer", 1, "buffer size in Mb")
	flag.IntVar(&linelength, "linelength", 17, "length of the line (length of each number + 1 for newline)")
	flag.BoolVar(&version, "version", false, "print version and exit")
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
	res := b / 1024 / 1024
	return res
}

func writeToFile(ctx context.Context, filename string, goroutines, dataPerGoroutine int) error {
	f, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	bufferByteSize := bufferSize * 1024 * 1024
	bf := bufio.NewWriterSize(f, bufferByteSize)
	err = write(ctx, bf, goroutines, dataPerGoroutine)
	if err != nil {
		return err
	}
	return bf.Flush()
}

func write(ctx context.Context, w io.Writer, goroutines, dataPerGoroutine int) error {
	errs, _ := errgroup.WithContext(ctx)
	var filelock sync.Mutex
	n := bufferSize * 1024 * 4 // number of lines in 1 buffered batch

	for i := 0; i < goroutines; i++ {
		errs.Go(func() error {
			r := rand.New(rand.NewSource(time.Now().UnixNano()))
			randomBytes := make([]byte, 8*n)
			r.Read(randomBytes)
			randomHexDigits := make([]byte, (linelength-1)*n)
			outputBuffer := make([]byte, 0, linelength*n+1) // 1 '\n' after every 16 digits

			flushRefreshReuse := func() error {
				// Flush to w
				filelock.Lock()
				_, err := w.Write(outputBuffer)
				filelock.Unlock()
				// Refresh work buffers with new random bytes
				r.Read(randomBytes)
				hex.Encode(randomHexDigits, randomBytes)
				// Reuse output buffer
				outputBuffer = outputBuffer[:0]
				return err
			}

			for j := 0; j < dataPerGoroutine; j++ {
				k := j % n
				if k == 0 {
					if err := flushRefreshReuse(); err != nil {
						return err
					}
				}
				outputBuffer = append(outputBuffer, randomHexDigits[(linelength-1)*k:(linelength-1)*k+(linelength-1)]...)
				outputBuffer = append(outputBuffer, '\n')
			}

			return flushRefreshReuse()
		})
	}

	return errs.Wait()
}

func main() {
	flag.Parse()
	if version {
		fmt.Println(VERSION)
		os.Exit(0)
	}

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

	fmt.Println(linelength * count)
	fmt.Printf("total count: %d\ngoroutine: %d\n", count, goroutine)
	fmt.Printf("gen per goroutine: %d\n", dataPerGoroutine)
	fmt.Printf("total gen: %dMB\n", bToMb(uint64(count*linelength)))

	fmt.Println()

	defer duration("gen number", time.Now())
	err := writeToFile(context.Background(), filename, goroutine, dataPerGoroutine)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	PrintMemUsage()
	fmt.Println()
}
