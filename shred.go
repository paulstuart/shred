package main

import (
	"bufio"
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"path"
	"runtime"
	"sync"
	"time"

	"golang.org/x/exp/mmap"
	"golang.org/x/sync/semaphore"
)

var ()

const fileTemplate = "%s/%s-%04d-%012d-%012d%s"

func main() {
	var (
		size    int64 = 1 << 30
		skip    int
		workers = runtime.GOMAXPROCS(0)
		prefix  = "part"
	)

	flag.Int64Var(&size, "size", size, "file size for each chunk")
	flag.IntVar(&skip, "skip", skip, "skip # lines from beginning of file")
	flag.IntVar(&workers, "workers", workers, "number of simultaneous workers")
	flag.StringVar(&prefix, "prefix", prefix, "prefix of chunked files")
	flag.Parse()

	args := flag.Args()
	if len(args) < 2 {
		log.Fatalf("usage: %s src-file dest-dir", os.Args[0])
	}
	filename := args[0]
	dir := args[1]
	now := time.Now()
	if err := ChunkFile(filename, dir, prefix, size, workers, skip); err != nil {
		log.Fatal(err)
	}
	log.Println("elapsed time:", time.Since(now))

}

// return a list of sections of ~ size
// it will check at the size offset, then work back until
// it finds a newline
func chunkyBySize(mf *mmap.ReaderAt, size int64) ([]Section, error) {
	var sections []Section
	fsize := int64(mf.Len())
	if size > fsize {
		sections = append(sections, Section{0, fsize})
		return sections, nil
	}

	var offset int64
	page := int64(4096)
	if page > size {
		page = size
	}
	buf := make([]byte, page)
	for offset < fsize {
		next := offset + size
		if next > fsize {
			next = fsize
		} else {
			// get the final page of this secton
			n, err := mf.ReadAt(buf, next-page)
			if err != nil {
				return sections, err
			}
			var last int64
			if n < int(page) {
				// not a full page so that's as close to the end as we get
				last = int64(n)
			} else {
				last = int64(bytes.LastIndexByte(buf, byte('\n')))
			}
			next = offset + size - page + last
		}
		sections = append(sections, Section{offset, next})
		log.Printf("chunk from %016d:%012d (%16d)\n", offset, next, next-offset)
		offset = next + 1
	}
	return sections, nil
}

// ChunksBySize returns a list of offsets of text sized to be at or under
// the given size. It will be adjusted to split on a newline.
func ChunksBySize(filename string, size int64) ([]Section, error) {
	var sections []Section
	mf, err := mmap.Open(filename)
	if err != nil {
		return sections, err
	}
	defer mf.Close()

	return chunkyBySize(mf, size)
}

// Section is a tuple of memory offset and size
type Section struct {
	off int64
	end int64
}

// Carve is a filewriter helper
func Carve(r io.Reader, filename string) error {
	f, err := os.Create(filename)
	if err != nil {
		return err
	}
	const WriteBufferSize = 16777216 // 32768 // 65536
	w := bufio.NewWriterSize(f, WriteBufferSize)
	if _, err := io.Copy(w, r); err != nil {
		return err
	}
	if err = w.Flush(); err != nil {
		return err
	}
	return f.Close()
}

func skipLines(mf *mmap.ReaderAt, lines int) (int64, error) {
	buf := make([]byte, 1<<16) // 1 megabyte should be enough :-)
	n, err := mf.ReadAt(buf, 0)
	if err != nil {
		return 0, err
	}
	buf = buf[:n]
	var offset int64
	for lines > 0 {
		lines--
		idx := bytes.Index(buf, []byte("\n"))
		if idx < 0 {
			return 0, fmt.Errorf("newline not found")
		}
		idx++
		offset += int64(idx)
		buf = buf[idx:]
	}
	return offset, nil
}

// FileChunks splits the given files into smaller chunks,
// as specified by each section offset and endpoint
func FileChunks(source, dir, prefix string, workers, skip int, sections []Section) error {
	mf, err := mmap.Open(source)
	if err != nil {
		return err
	}

	defer mf.Close()
	ctx := context.TODO()
	sem := semaphore.NewWeighted(int64(workers))
	log.Printf("chunkng with %d threads for %d sections\n", workers, len(sections))

	ext := path.Ext(source)
	var wg sync.WaitGroup
	wg.Add(len(sections))
	for i, s := range sections {
		filename := fmt.Sprintf(fileTemplate, dir, prefix, i, s.off, s.end, ext)
		if err := sem.Acquire(ctx, 1); err != nil {
			log.Printf("Failed to acquire semaphore: %v", err)
			break
		}
		if i == 0 && skip > 0 {
			idx, err := skipLines(mf, skip)
			if err != nil {
				return fmt.Errorf("failed to skip lines: %w", err)
			}
			s.off = idx
		}
		r := Segment(mf, s.off, s.end)
		go func(r io.Reader, idx int, f string) {
			if err := Carve(r, f); err != nil {
				log.Printf("error carving to file %q: %v", f, err)
			}
			sem.Release(1)
			wg.Done()
		}(r, i, filename)
	}
	wg.Wait()
	return nil
}

// ChunkFile splits filename into size chunks into dir
func ChunkFile(filename, dir, prefix string, size int64, workers, skip int) error {
	if err := os.MkdirAll(dir, fs.ModePerm); err != nil {
		return err
	}
	list, err := ChunksBySize(filename, size)
	if err != nil {
		fmt.Println("chunk funk:", err)
	}
	return FileChunks(filename, dir, prefix, workers, skip, list)
}

type mreader struct {
	mm     *mmap.ReaderAt
	offset int64
	size   int64
}

func (m *mreader) Read(b []byte) (int, error) {
	if len(b) > int(m.size) {
		b = b[:m.size]
	}
	n, err := m.mm.ReadAt(b, m.offset)
	m.offset += int64(n)
	m.size -= int64(n)
	if m.size == 0 && err == nil {
		err = io.EOF
	}
	return n, err
}

// Segment take a chunk of a memory mapped file and returns an io.Reader
func Segment(mm *mmap.ReaderAt, offset, end int64) io.Reader {
	size := end - offset + 1
	return &mreader{mm, offset, size}
}
