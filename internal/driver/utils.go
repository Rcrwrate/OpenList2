package driver

import (
	"context"
	"io"

	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/stream"
	"github.com/OpenListTeam/OpenList/v4/pkg/http_range"
)

type UpdateProgress = model.UpdateProgress

type Progress struct {
	Total int64
	Done  int64
	up    UpdateProgress
}

func (p *Progress) Write(b []byte) (n int, err error) {
	n = len(b)
	p.Done += int64(n)
	p.up(float64(p.Done) / float64(p.Total) * 100)
	return
}

func NewProgress(total int64, up UpdateProgress) *Progress {
	return &Progress{
		Total: total,
		up:    up,
	}
}

type RateLimitReader = stream.RateLimitReader

type RateLimitWriter = stream.RateLimitWriter

type RateLimitFile = stream.RateLimitFile

func NewLimitedUploadStream(ctx context.Context, r io.Reader) *RateLimitReader {
	return &RateLimitReader{
		Reader:  r,
		Limiter: stream.ServerUploadLimit,
		Ctx:     ctx,
	}
}

func NewLimitedUploadFile(ctx context.Context, f model.File) *RateLimitFile {
	return &RateLimitFile{
		File:    f,
		Limiter: stream.ServerUploadLimit,
		Ctx:     ctx,
	}
}

func ServerUploadLimitWaitN(ctx context.Context, n int) error {
	return stream.ServerUploadLimit.WaitN(ctx, n)
}

type ReaderWithCtx = stream.ReaderWithCtx

type ReaderUpdatingProgress = stream.ReaderUpdatingProgress

type SimpleReaderWithSize = stream.SimpleReaderWithSize

func GetRangeReadCloserFromMFile(size int64, file model.File) model.RangeReadCloserIF {
	rrc := &model.RangeReadCloser{RangeReader: func(ctx context.Context, httpRange http_range.Range) (io.ReadCloser, error) {
		length := httpRange.Length
		if length < 0 || httpRange.Start+length > size {
			length = size - httpRange.Start
		}
		return io.NopCloser(io.NewSectionReader(file, httpRange.Start, length)), nil
	}}
	rrc.AddIfCloser(file)
	return rrc
}
