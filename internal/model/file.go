package model

import "io"

// File is basic file level accessing interface
type File interface {
	io.Reader
	io.ReaderAt
	io.Seeker
}
type FileCloser struct {
	File
}

func (f *FileCloser) Close() error {
	if clr, ok := f.File.(io.Closer); ok {
		return clr.Close()
	}
	return nil
}
