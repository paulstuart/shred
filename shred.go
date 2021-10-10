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
	"strconv"
	"sync"

	"golang.org/x/exp/mmap"
	"golang.org/x/sync/semaphore"
)

func main() {
	var size int64 = 1 << 30
	flag.Int64Var(&size, "size", size, "file size for each chunk")
	flag.Parse()

	args := flag.Args()
	if len(args) < 2 {
		log.Fatalf("usage: %s src-file dest-dir", os.Args[0])
	}
	filename := args[0]
	dir := args[1]
	if err := ChunkFile(filename, dir, size); err != nil {
		log.Fatal(err)
	}

}

type saver func(offset, size int64) (io.Writer, error)

var (
	maxWorkers = runtime.GOMAXPROCS(0)
	out        = make([]int, 32)
)

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
		fmt.Printf("From %016d:%012d (%16d)\n", offset, next, next-offset)
		offset = next + 1
	}
	return sections, nil
}

func chunkyByCount(mf *mmap.ReaderAt, count int64) ([]Section, error) {
	const page = 4096

	var sections []Section
	fsize := int64(mf.Len())
	size := (fsize / count) + page

	var offset int64
	buf := make([]byte, page)
	counter := 0
	for offset < fsize {
		if false {
			// debugging only
			line, err := mmappedLine(mf, offset)
			if err != nil {
				return nil, err
			}
			fmt.Printf("SECT (%d@%d): %s\n", counter, offset, line)
		}

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
				last = int64(bytes.LastIndexByte(buf, '\n'))
			}
			next = offset + size - page + last
		}

		sections = append(sections, Section{offset, next})
		fmt.Printf("From %016d:%012d (%16d)\n", offset, next, next-offset)
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

// ChunksByCount returns a list of offsets of text sized to be at or under
// the given size. It will be adjusted to split on a newline.
func ChunksByCount(filename string, count int64) ([]Section, error) {
	//var sections []Section
	mf, err := mmap.Open(filename)
	if err != nil {
		return nil, err
	}
	defer mf.Close()

	return chunkyByCount(mf, count)
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

const fileTemplate = "%s/part-%04d-%012d-%012d%s"

// FileChunks splits the given files into smaller chunks,
// as specified by each section offset and endpoint
func FileChunks(source, dir string, sections []Section) error {
	mf, err := mmap.Open(source)
	if err != nil {
		return err
	}

	defer mf.Close()
	ctx := context.TODO()
	sem := semaphore.NewWeighted(int64(maxWorkers))
	log.Printf("chunkng with %d threads for %d sections\n", maxWorkers, len(sections))

	ext := path.Ext(source)
	var wg sync.WaitGroup
	wg.Add(len(sections))
	for i, s := range sections {
		filename := fmt.Sprintf(fileTemplate, dir, i, s.off, s.end, ext)
		if err := sem.Acquire(ctx, 1); err != nil {
			log.Printf("Failed to acquire semaphore: %v", err)
			break
		}
		r := Segment(mf, s.off, s.end)
		go func(r io.Reader, f string) {
			if err := Carve(r, f); err != nil {
				log.Printf("error carving to file %q: %v", f, err)
			}
			sem.Release(1)
			wg.Done()
		}(r, filename)
	}
	wg.Wait()
	return nil
}

// ChunkFile splits filename into size chunks into dir
func ChunkFile(filename, dir string, size int64) error {
	if err := os.MkdirAll(dir, fs.ModePerm); err != nil {
		return err
	}
	list, err := ChunksBySize(filename, size)
	if err != nil {
		fmt.Println("chunk funk:", err)
	}
	return FileChunks(filename, dir, list)
}

func chunktest() {
	if len(os.Args) < 4 {
		log.Fatalf("usage: %s <filename> <dest-dir> <size>", os.Args[0])
	}
	file := os.Args[1]
	dir := os.Args[2]
	stext := os.Args[3]
	size, err := strconv.ParseInt(stext, 10, 64)
	if err != nil {
		log.Fatal(err)
	}
	if err = ChunkFile(file, dir, size); err != nil {
		log.Fatal(err)
	}
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

type sectionReader struct {
	r  io.Reader
	wg *sync.WaitGroup
}

func (s *sectionReader) Read(b []byte) (int, error) {
	n, err := s.r.Read(b)
	if err == io.EOF {
		s.wg.Done()
	}
	return n, err
}

// Segment take a chunk of a memory mapped file and returns an io.Reader
func Segment(mm *mmap.ReaderAt, offset, end int64) io.Reader {
	size := end - offset + 1
	return &mreader{mm, offset, size}
}

// ChunkReaders splits the given file into multiple readers,
// each reading <size> amount of the file (memory mapped)
func ChunkReaders(filename string, count int) ([]io.Reader, error) {
	sections, err := ChunksByCount(filename, int64(count))
	if err != nil {
		return nil, err
	}

	mf, err := mmap.Open(filename)
	if err != nil {
		return nil, err
	}

	var wg sync.WaitGroup
	wg.Add(len(sections))

	readers := make([]io.Reader, len(sections))
	for i, section := range sections {
		r := Segment(mf, section.off, section.end)
		readers[i] = &sectionReader{r, &wg}
	}

	go func() {
		wg.Wait()
		fmt.Println("closing:", filename)
	}()

	return readers, nil
}

// FOR TESTIBNG
func Offsets(sections ...Section) []int64 {
	list := make([]int64, len(sections))
	for i, section := range sections {
		list[i] = section.off
	}
	return list
}

func FirstLines(filename string, offsets ...int64) ([]string, error) {
	mf, err := mmap.Open(filename)
	if err != nil {
		return nil, err
	}
	defer mf.Close()

	list := make([]string, len(offsets))
	for i, offset := range offsets {
		line, err := mmappedLine(mf, offset)
		if err != nil {
			return nil, err
		}
		list[i] = line
	}
	return list, nil
}

func mmappedLine(mf *mmap.ReaderAt, offset int64) (string, error) {
	buf := make([]byte, PageSize)
	n, err := mf.ReadAt(buf, offset)
	if err != nil {
		return "", err
	}
	buf = buf[:n]
	i := bytes.IndexByte(buf, '\n')
	if i < 0 {
		return "", fmt.Errorf("no line end in buffer")
	}
	return string(buf[:i]), nil
}

func FunkyReaders(filename string, count int) ([]io.Reader, error) {
	sections, err := ChunksByCount(filename, int64(count))
	if err != nil {
		return nil, err
	}

	var wg sync.WaitGroup
	wg.Add(len(sections))

	readers := make([]io.Reader, len(sections))
	for i, section := range sections {
		f, err := os.Open(filename)
		if err != nil {
			return nil, err
		}

		r := Pigment(f, section.off, section.end)
		readers[i] = &sectionReader{r, &wg}
	}

	go func() {
		wg.Wait()
		fmt.Println("closing:", filename)
		for _, r := range readers {
			if c, ok := r.(io.Closer); ok {
				c.Close()
			}
		}
	}()

	return readers, nil
}

// Pigment take a chunk of a memory mapped file and returns an io.Reader
func Pigment(mm io.ReaderAt, offset, end int64) io.Reader {
	size := end - offset + 1
	return &atReader{mm, offset, size}
}

type atReader struct {
	mm     io.ReaderAt
	offset int64
	size   int64
}

func (m *atReader) Read(b []byte) (int, error) {
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
