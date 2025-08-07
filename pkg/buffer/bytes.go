package buffer

import (
	"errors"
	"io"
)

type Bytes struct {
	bytes  [][]byte
	length int
	offset int
}

func (b *Bytes) Len() int {
	return b.length
}

func (b *Bytes) Append(buf []byte) {
	b.length += len(buf)
	b.bytes = append(b.bytes, buf)
}

func (b *Bytes) Read(p []byte) (int, error) {
	n, err := b.ReadAt(p, int64(b.offset))
	if n > 0 {
		b.offset += n
	}
	return n, err
}

func (b *Bytes) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 || off >= int64(b.length) {
		return 0, io.EOF
	}

	n, right := 0, int64(0)
	readFrom := false
	for idx := range b.bytes {
		newRight := right + int64(len(b.bytes[idx]))
		if readFrom {
			w := copy(p[n:], b.bytes[idx])
			n += w
		} else if off < newRight {
			readFrom = true
			bufOffset := int(off - right)
			w := copy(p[n:], b.bytes[idx][bufOffset:])
			n += w
		}
		right = newRight
		if n == len(p) {
			return n, nil
		}
	}

	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func (b *Bytes) Seek(offset int64, whence int) (int64, error) {
	var abs int
	switch whence {
	case io.SeekStart:
		abs = int(offset)
	case io.SeekCurrent:
		abs = b.offset + int(offset)
	case io.SeekEnd:
		abs = b.length + int(offset)
	default:
		return 0, errors.New("Seek: invalid whence")
	}

	if abs < 0 {
		return 0, errors.New("Seek: invalid offset")
	}

	b.offset = abs
	return int64(abs), nil
}

func (b *Bytes) Reset() {
	clear(b.bytes)
	b.bytes = nil
	b.length = 0
	b.offset = 0
}
