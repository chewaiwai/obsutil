package ratelimit

import (
	"io"
	"time"
)

type RateLimitReader struct {
	Reader  io.Reader
	Limiter *RateLimiter
}

func NewRateLimitReader(reader io.Reader, capacity, rate int64) *RateLimitReader {
	rlr := &RateLimitReader{}
	rlr.Reader = reader
	rlr.Limiter = NewRateLimiter(capacity, rate)
	return rlr
}

func NewRateLimitReaderWithLimiter(reader io.Reader, limiter *RateLimiter) *RateLimitReader {
	rlr := &RateLimitReader{}
	rlr.Reader = reader
	rlr.Limiter = limiter
	return rlr
}

func (rlr *RateLimitReader) Read(p []byte) (n int, err error) {
	count := len(p)
	var ret int64
	for {
		ret = rlr.Limiter.Acquire(int64(count))
		if ret == 0 {
			break
		}
		time.Sleep(time.Millisecond * time.Duration(ret))
	}
	return rlr.Reader.Read(p)
}
