package ftp

import (
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/OpenListTeam/OpenList/v4/pkg/singleflight"
	"github.com/jlaffaye/ftp"
)

// do others that not defined in Driver interface

func (d *FTP) login() error {
	err, _, _ := singleflight.ErrorGroup.Do(fmt.Sprintf("FTP.login:%p", d), func() (error, error) {
		return d._login(), nil
	})
	return err
}

func (d *FTP) _login() error {

	if d.conn != nil {
		_, err := d.conn.CurrentDir()
		if err == nil {
			return nil
		}
	}
	conn, err := ftp.Dial(d.Address, ftp.DialWithShutTimeout(10*time.Second))
	if err != nil {
		return err
	}
	err = conn.Login(d.Username, d.Password)
	if err != nil {
		return err
	}
	d.conn = conn
	return nil
}

// FileReader An FTP file reader that implements io.MFile for seeking.
type FileReader struct {
	conn    *ftp.ServerConn
	respMap sync.Map
	offset  int64
	path    string
	size    int64
}

func NewFileReader(conn *ftp.ServerConn, path string, size int64) *FileReader {
	return &FileReader{
		conn: conn,
		path: path,
		size: size,
	}
}

func (r *FileReader) Read(buf []byte) (n int, err error) {
	n, err = r.ReadAt(buf, r.offset)
	r.offset += int64(n)
	return
}

func (r *FileReader) ReadAt(buf []byte, off int64) (n int, err error) {
	if off < 0 {
		return -1, os.ErrInvalid
	}
	rcRaw, _ := r.respMap.LoadAndDelete(off)
	resp, ok := rcRaw.(*ftp.Response)
	if !ok {
		resp, err = r.conn.RetrFrom(r.path, uint64(off))
		if err != nil {
			return
		}
	}
	n, err = resp.Read(buf)
	off += int64(n)
	if err == nil {
		r.respMap.Store(off, resp)
	}
	return
}

func (r *FileReader) Seek(offset int64, whence int) (int64, error) {
	oldOffset := r.offset
	var newOffset int64
	switch whence {
	case io.SeekStart:
		newOffset = offset
	case io.SeekCurrent:
		newOffset = oldOffset + offset
	case io.SeekEnd:
		return r.size, nil
	default:
		return -1, os.ErrInvalid
	}

	if newOffset < 0 {
		// offset out of range
		return oldOffset, os.ErrInvalid
	}
	if newOffset == oldOffset {
		// offset not changed, so return directly
		return oldOffset, nil
	}
	r.offset = newOffset
	return newOffset, nil
}

func (r *FileReader) Close() error {
	var errs []error
	r.respMap.Range(func(key, value any) bool {
		if resp, ok := value.(*ftp.Response); ok {
			errs = append(errs, resp.Close())
		}
		return true
	})
	r.respMap.Clear()
	return errors.Join(errs...)
}
