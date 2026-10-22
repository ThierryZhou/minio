package cmd

import (
	"bytes"
	"io"
	"testing"

	"github.com/minio/minio/internal/bpool"
)

// The pool's backing array must reach the actual shard reads, not just Done.
func TestParallelReaderPoolBacking(t *testing.T) {
	for _, tc := range []struct {
		name          string
		capacity      int
		pooled, retry bool
	}{
		{"pooled", 2048, true, false},
		{"pooled-retry", 2048, true, true},
		{"undersized", 1024, false, false},
		{"no-pool", 0, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			old := globalBytePoolCap.Load()
			defer globalBytePoolCap.Store(old)
			var pool *bpool.BytePoolCap
			if tc.capacity != 0 {
				pool = bpool.NewBytePoolCap(1, tc.capacity, tc.capacity)
				pool.Populate()
			}
			globalBytePoolCap.Store(pool)
			e, err := NewErasure(t.Context(), 2, 2, 1024)
			if err != nil {
				t.Fatal(err)
			}
			const total = int64(4097) // Also exercises a short final shard.
			payload := bytes.Repeat([]byte{0x6d}, int(e.ShardFileSize(total)))
			readers := make([]io.ReaderAt, 4)
			for i := range readers {
				readers[i] = bytes.NewReader(payload)
			}
			if tc.retry {
				readers[0] = nil
			}
			p := newParallelReader(readers, e, 0, total)
			defer p.Done()
			if tc.pooled {
				if len(p.stashBuffer) == 0 {
					t.Fatal("missing pooled backing buffer")
				}
				for i := range p.buf {
					if len(p.buf[i]) != int(e.ShardSize()) {
						t.Fatalf("shard %d not seeded from pool", i)
					}
					if &p.buf[i][0] != &p.stashBuffer[i*int(e.ShardSize())] {
						t.Fatalf("shard %d has different backing array", i)
					}
				}
			} else if p.stashBuffer != nil {
				t.Fatal("unexpected pooled backing")
			}
			var bufs [][]byte
			for offset := int64(0); offset < int64(len(payload)); offset += e.ShardSize() {
				bufs, err = p.Read(bufs)
				if err != nil {
					t.Fatal(err)
				}
				n := 0
				end := offset + e.ShardSize()
				if end > int64(len(payload)) {
					end = int64(len(payload))
				}
				for i, buf := range bufs {
					if len(buf) == 0 {
						continue
					}
					n++
					if !bytes.Equal(buf, payload[offset:end]) {
						t.Fatalf("shard %d wrong data at %d", i, offset)
					}
					if tc.pooled && &buf[0] != &p.stashBuffer[i*int(e.ShardSize())] {
						t.Fatalf("shard %d allocated outside pool", i)
					}
				}
				if n != 2 {
					t.Fatalf("got %d shards, want 2", n)
				}
			}
			p.Done()
			p.Done()
			if p.stashBuffer != nil {
				t.Fatal("backing buffer retained after Done")
			}
			if tc.pooled && pool.CurrentSize() != tc.capacity {
				t.Fatal("backing buffer was not returned")
			}
		})
	}
}

func TestParallelReaderPoolDecode(t *testing.T) {
	for _, tc := range []struct {
		name                 string
		missing, parityFirst bool
		offset, length       int64
	}{
		{"healthy-short-tail", false, false, 0, 2085},
		{"missing-data-range", true, false, 13, 2072},
		{"parity-preferred-range", false, true, 777, 1308},
	} {
		t.Run(tc.name, func(t *testing.T) {
			old := globalBytePoolCap.Load()
			defer globalBytePoolCap.Store(old)
			pool := bpool.NewBytePoolCap(1, 2048, 2048)
			pool.Populate()
			globalBytePoolCap.Store(pool)
			e, err := NewErasure(t.Context(), 2, 2, 1024)
			if err != nil {
				t.Fatal(err)
			}
			payload := make([]byte, 2085)
			for i := range payload {
				payload[i] = byte(i*17 + i/251)
			}
			files := make([][]byte, 4)
			for offset := 0; offset < len(payload); offset += 1024 {
				end := offset + 1024
				if end > len(payload) {
					end = len(payload)
				}
				shards, err := e.EncodeData(t.Context(), bytes.Clone(payload[offset:end]))
				if err != nil {
					t.Fatal(err)
				}
				for i := range files {
					files[i] = append(files[i], shards[i]...)
				}
			}
			readers := make([]io.ReaderAt, 4)
			for i := range readers {
				readers[i] = bytes.NewReader(files[i])
			}
			if tc.missing {
				readers[0] = nil
			}
			prefer := []bool{true, true, false, false}
			if tc.parityFirst {
				prefer = []bool{false, false, true, true}
			}
			var out bytes.Buffer
			n, err := e.Decode(t.Context(), &out, readers, tc.offset, tc.length, int64(len(payload)), prefer)
			if err != nil {
				t.Fatal(err)
			}
			if n != tc.length || !bytes.Equal(out.Bytes(), payload[tc.offset:tc.offset+tc.length]) {
				t.Fatal("decoded bytes differ")
			}
			if pool.CurrentSize() != 2048 {
				t.Fatal("Decode did not release pooled buffer")
			}
		})
	}
}

// Standalone storage tests need a pool: their streaming writers use it too.
func TestParallelReaderPoolStorageDecode(t *testing.T) {
	old := globalBytePoolCap.Load()
	defer globalBytePoolCap.Store(old)
	globalBytePoolCap.Store(bpool.NewBytePoolCap(16, 1<<20, 4<<20))
	t.Run("existing-erasure-decode-cases", TestErasureDecode)
}

// Measures a complete 4MiB healthy read with the same 2+2/512KiB geometry as S3.
func BenchmarkParallelReaderPoolReuse(b *testing.B) {
	old := globalBytePoolCap.Load()
	defer globalBytePoolCap.Store(old)
	pool := bpool.NewBytePoolCap(1, 1<<20, 2<<20)
	pool.Populate()
	globalBytePoolCap.Store(pool)
	e, err := NewErasure(b.Context(), 2, 2, 1<<20)
	if err != nil {
		b.Fatal(err)
	}
	const total = int64(4 << 20)
	payload := make([]byte, e.ShardFileSize(total))
	readers := make([]io.ReaderAt, 4)
	for i := range readers {
		readers[i] = bytes.NewReader(payload)
	}
	b.ReportAllocs()
	b.SetBytes(total)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p := newParallelReader(readers, e, 0, total)
		var bufs [][]byte
		for offset := int64(0); offset < int64(len(payload)); offset += e.ShardSize() {
			bufs, err = p.Read(bufs)
			if err != nil {
				b.Fatal(err)
			}
		}
		p.Done()
	}
}
