package httptrace

import (
	"context"
	"net/http/httptrace"
	"time"
)

type Tracer struct {
	connStart time.Time
	respStart time.Time
}

func NewTracer() *Tracer {
	return &Tracer{}
}

func (t *Tracer) GetLatency() int64 {
	return t.respStart.Sub(t.connStart).Milliseconds()
}

func WithClientTrace(ctx context.Context, tracer *Tracer) context.Context {
	if tracer == nil {
		tracer = new(Tracer)
	}

	return httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{
		GetConn: func(hostPort string) {
			tracer.connStart = time.Now()
		},

		GotFirstResponseByte: func() {
			tracer.respStart = time.Now()
		},
	})
}
