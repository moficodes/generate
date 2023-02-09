package main

import (
	"bytes"
	"context"
	"io"
	"os"
	"strconv"
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

func BenchmarkDiscard10_000_4workers(b *testing.B) {
	ctx := context.Background()
	for i := 0; i < b.N; i++ {
		write(ctx, io.Discard, 4, 2_500)
	}
}

func BenchmarkDiscard10_000_50workers(b *testing.B) {
	ctx := context.Background()
	for i := 0; i < b.N; i++ {
		write(ctx, io.Discard, 50, 200)
	}
}

func TestNumberHex(t *testing.T) {
	buffer := make([]byte, 0, 256)
	for _, tc := range []struct {
		in       int64
		expected string
	}{
		{0, "0000000000000000000"},
		{1, "0000000000000000001"},
		{10, "000000000000000000a"},
		{15, "000000000000000000f"},
		{42, "000000000000000002a"},
	} {
		out1 := generateNumberHex(tc.in)
		if string(out1) != tc.expected {
			t.Errorf("generateNumberHex(%d): expected %q, got %q", tc.in, tc.expected, string(out1))
		}

		buffer = buffer[:0]
		buffer = appendNumberHex(tc.in, buffer)
		if string(buffer) != tc.expected {
			t.Errorf("encodeNumberHex(%d): expected %q, got %q", tc.in, tc.expected, string(buffer))
		}

		buffer = buffer[:0]
		buffer = appendInt16(tc.in, buffer)
		if string(buffer) != tc.expected {
			t.Errorf("appendInt16(%d): expected %q, got %q", tc.in, tc.expected, string(buffer))
		}
	}
}

func BenchmarkGenerateNumberHex(b *testing.B) {
	for i := 0; i < b.N; i++ {
		buffer := make([]byte, 0, 1*1024*1024)
		for k := 0; k < 20_000; k++ {
			number := int64(k * 17)
			buffer = buffer[:0]
			SinkBuf = generateNumberHex(number)
		}
	}
}

func BenchmarkAppendNumberHex(b *testing.B) {
	for i := 0; i < b.N; i++ {
		buffer := make([]byte, 0, 1*1024*1024)
		for k := 0; k < 20_000; k++ {
			number := int64(k * 17)
			buffer = buffer[:0]
			buffer = appendNumberHex(number, buffer)
		}
	}
}

func BenchmarkStrconvAppendInt(b *testing.B) {
	for i := 0; i < b.N; i++ {
		buffer := make([]byte, 0, 1*1024*1024)
		for k := 0; k < 20_000; k++ {
			number := int64(k * 17)
			buffer = buffer[:0]
			buffer = strconv.AppendInt(buffer, number, 16)
		}
	}
}

func BenchmarkAppendInt16(b *testing.B) {
	for i := 0; i < b.N; i++ {
		buffer := make([]byte, 0, 1*1024*1024)
		for k := 0; k < 20_000; k++ {
			number := int64(k * 17)
			buffer = buffer[:0]
			buffer = appendInt16(number, buffer)
		}
	}
}

// SinkBuf is a global var to prevent spurious "optimizing away" unused results
var SinkBuf []byte
