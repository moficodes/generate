package main

import (
	"bytes"
	"context"
	"io"
	"os"
	"testing"
)

func BenchmarkWrite10_000_seq(b *testing.B) {
	ctx := context.Background()
	for i := 0; i < b.N; i++ {
		var buffer bytes.Buffer
		write(ctx, &buffer, 1, 10_000)
	}
}

func BenchmarkWriteFile10_000_seq(b *testing.B) {
	ctx := context.Background()
	for i := 0; i < b.N; i++ {
		tmpfile, err := os.CreateTemp("", "")
		if err != nil {
			b.Fatal(err)
		}
		filename := tmpfile.Name()

		writeToFile(ctx, filename, 1, 10_000)
	}
}

func BenchmarkDiscard10_000_seq(b *testing.B) {
	ctx := context.Background()
	for i := 0; i < b.N; i++ {
		write(ctx, io.Discard, 1, 10_000)
	}
}
