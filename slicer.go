package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"path"
	"strconv"
	"strings"
	//	"golang.org/x/exp/mmap"
)

const PageSize = 4096

type ReaderAt interface {
	At(i int) byte
	Close() error
	Len() int
	ReadAt(b []byte, off int64) (int, error)
}

func abs(n int) int {
	if n < 0 {
		return -n
	}
	return n
}
func keyVal(line string) (int, error) {
	i := strings.IndexByte(line, ',')
	if i < 0 {
		return -1, fmt.Errorf("no key for line: %q", line)
	}

	f, err := strconv.ParseFloat(line[:i], 64)
	if err != nil {
		return -1, err
	}
	return abs(int(f)), nil
}

func XkeyVal(s string) (int, error) {
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return -1, err
	}
	return abs(int(f)), nil
}

func getKey(b []byte) string {
	i := bytes.IndexByte(b, ',')
	if i > 0 {
		return string(b[:i])
	}
	return ""
}

func keyText(r io.Reader) string {
	buf := make([]byte, PageSize)
	n, _ := r.Read(buf)
	buf = buf[:n]
	return getKey(buf)
}

func Slicer(filename, dir string) error {
	mf, err := os.Open(filename)
	if err != nil {
		return err
	}

	defer mf.Close()
	/*
		size := mf.Len()
		fmt.Printf("Slicing %s (%d)\n", filename, size)
	*/

	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
		return err
	}
	segment := func(key int) (io.WriteCloser, error) {
		name := path.Join(dir, fmt.Sprintf("group-%06d.csv", key))
		return os.Create(name)
	}

	var last int
	var w io.WriteCloser
	scanner := bufio.NewScanner(mf)
	for scanner.Scan() {
		line := scanner.Text()
		key, err := keyVal(line)
		if err != nil {
			return err
		}
		if last != key {
			if last > 0 {
				if err = w.Close(); err != nil {
					return err
				}
			}
			if w, err = segment(key); err != nil {
				return err
			}
		}
		fmt.Fprintln(w, line)
		last = key
	}
	return w.Close()
}
