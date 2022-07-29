package edge

import (
	"fmt"
	"io"
	"time"

	"golang.org/x/time/rate"
)

// type reader struct {
// 	r       io.Reader
// 	limiter *rate.Limiter
// }

// // Reader returns a reader that is rate limited by
// // the given token bucket. Each token in the bucket
// // represents one byte.
// func NewRateLimitReader(r io.Reader, speed, burst int) io.Reader {
// 	return &reader{
// 		r:       r,
// 		limiter: rate.NewLimiter(rate.Limit(speed), burst),
// 	}
// }

// func (r *reader) Read(buf []byte) (int, error) {
// 	n, err := r.r.Read(buf)
// 	if n <= 0 {
// 		return n, err
// 	}

// 	ctx := context.Background()
// 	r.limiter.WaitN(ctx, n)
// 	return n, err
// }

type reader struct {
	r       io.Reader
	limiter *rate.Limiter
}

// NewReader returns a reader that is rate limited by
// the given token bucket. Each token in the bucket
// represents one byte.
func NewReader(r io.Reader, l *rate.Limiter) io.Reader {
	return &reader{
		r:       r,
		limiter: l,
	}
}

func (r *reader) Read(buf []byte) (int, error) {
	n, err := r.r.Read(buf)
	if n <= 0 {
		return n, err
	}

	now := time.Now()
	rv := r.limiter.ReserveN(now, n)
	if !rv.OK() {
		return 0, fmt.Errorf("Exceeds limiter's burst")
	}
	delay := rv.DelayFrom(now)
	//fmt.Printf("Read %d bytes, delay %d\n", n, delay)
	time.Sleep(delay)
	return n, err
}
