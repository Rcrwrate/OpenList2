package model

import (
	"errors"
	"io"
)

// File is basic file level accessing interface
type File interface {
	io.Reader
	io.ReaderAt
	io.Seeker
}
type FileCloser struct {
	File
	io.Closer
}

func (f *FileCloser) Close() error {
	var err error
	if clr, ok := f.File.(io.Closer); ok {
		err = errors.Join(err, clr.Close())
	}
	if f.Closer != nil {
		err = errors.Join(err, f.Closer.Close())
	}
	return err
}

type FileRangeReader struct {
	RangeReaderIF
}
