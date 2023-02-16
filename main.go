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
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

var (
	count         int
	goroutine     int
	filename      string
	bufferSize    int
	version       bool
	linelength    int
	filenameIndex string
	VERSION       string = "v0.0.0"
)

var log *logrus.Logger
var dataPerGoroutine int

func init() {
	log = logrus.New()
	log.Level = logrus.DebugLevel
	log.Formatter = &logrus.JSONFormatter{
		FieldMap: logrus.FieldMap{
			logrus.FieldKeyTime:  "timestamp",
			logrus.FieldKeyLevel: "severity",
			logrus.FieldKeyMsg:   "message",
		},
		TimestampFormat: time.RFC3339Nano,
	}
	log.Out = os.Stdout
	flag.IntVar(&count, "count", 0, "number of record to generate")
	flag.IntVar(&goroutine, "goroutine", 0, "number of goroutine to run")
	flag.StringVar(&filename, "file", "input.txt", "name of the file")
	flag.IntVar(&bufferSize, "buffer", 1, "buffer size in Mb")
	flag.IntVar(&linelength, "linelength", 17, "length of the line (length of each number + 1 for newline)")
	flag.StringVar(&filenameIndex, "fileindex", "", "name of the file index")
	flag.BoolVar(&version, "version", false, "print version and exit")
}

func humanReadableFilesize(size int64) string {
	const unit = 1024
	if size < unit {
		return fmt.Sprintf("%d B", size)
	}
	div, exp := int64(unit), 0
	for n := size / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(size)/float64(div), "kMGTPE"[exp])
}

// PrintMemUsage outputs the current, total and OS memory being used. As well as the number
// of garage collection cycles completed.
func MemUsage() string {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	// For info on each, see: https://golang.org/pkg/runtime/#MemStats
	message := fmt.Sprintf("alloc = %s, totalAlloc = %s, sys = %s, numGC = %v", humanReadableFilesize(int64(m.Alloc)), humanReadableFilesize(int64(m.TotalAlloc)), humanReadableFilesize(int64(m.Sys)), m.NumGC)
	return message
}

func duration(msg string, start time.Time) {
	log.Infof("%s took %s", msg, time.Since(start))
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
	defer duration("gen number", time.Now())
	flag.Parse()

	logName := os.Getenv("HOSTNAME")
	if logName == "" {
		logName = "generate"
	}

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
		log.Error("no data to produce")
		os.Exit(1)
	}

	if filenameIndex != "" {
		index, err := strconv.Atoi(filenameIndex)
		if err != nil {
			log.Fatalln(err)
		}
		paths := strings.Split(filename, "/")
		file := paths[len(paths)-1]
		name := strings.Split(file, ".")[0]
		ext := strings.Split(file, ".")[1]
		if len(paths) == 1 {
			filename = fmt.Sprintf(("%s_%04d.%s"), name, index, ext)
		} else {
			filename = fmt.Sprintf(("%s/%s_%04d.%s"), strings.Join(paths[:len(paths)-1], "/"), name, index, ext)
		}

	}

	dataPerGoroutine = count / goroutine
	count = count - (count % goroutine)

	log.Infof("total count: %d, goroutine: %d, gen per goroutine: %d", count, goroutine, dataPerGoroutine)
	err := writeToFile(context.Background(), filename, goroutine, dataPerGoroutine)
	if err != nil {
		log.Error(err)
	}
	log.Infof("total gen: %s, filename: %s", humanReadableFilesize(int64(count*linelength)), filename)
	log.Debug(MemUsage())
}
