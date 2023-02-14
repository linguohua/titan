package metrics

import (
	"context"
	"time"

	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"

	rpcmetrics "github.com/filecoin-project/go-jsonrpc/metrics"
)

// Distribution
var defaultMillisecondsDistribution = view.Distribution(0.01, 0.05, 0.1, 0.3, 0.6, 0.8, 1, 2, 3, 4, 5, 6, 8, 10, 13, 16, 20, 25, 30, 40, 50, 65, 80, 100, 130, 160, 200, 250, 300, 400, 500, 650, 800, 1000, 2000, 3000, 4000, 5000, 7500, 10000, 20000, 50000, 100000)
var workMillisecondsDistribution = view.Distribution(
	250, 500, 1000, 2000, 5000, 10_000, 30_000, 60_000, 2*60_000, 5*60_000, 10*60_000, 15*60_000, 30*60_000, // short sealing tasks
	40*60_000, 45*60_000, 50*60_000, 55*60_000, 60*60_000, 65*60_000, 70*60_000, 75*60_000, 80*60_000, 85*60_000, 100*60_000, 120*60_000, // PC2 / C2 range
	130*60_000, 140*60_000, 150*60_000, 160*60_000, 180*60_000, 200*60_000, 220*60_000, 260*60_000, 300*60_000, // PC1 range
	350*60_000, 400*60_000, 600*60_000, 800*60_000, 1000*60_000, 1300*60_000, 1800*60_000, 4000*60_000, 10000*60_000, // intel PC1 range
)

// Global Tags
var (
	// common
	Version, _     = tag.NewKey("version")
	Commit, _      = tag.NewKey("commit")
	NodeType, _    = tag.NewKey("node_type")
	FailureType, _ = tag.NewKey("failure_type")

	Endpoint, _     = tag.NewKey("endpoint")
	APIInterface, _ = tag.NewKey("api") // to distinguish between gateway api and full node api endpoint calls
)

// Measures
var (
	// common
	TitanInfo          = stats.Int64("info", "Arbitrary counter to tag lotus info to", stats.UnitDimensionless)
	APIRequestDuration = stats.Float64("api/request_duration_ms", "Duration of API requests", stats.UnitMilliseconds)
)

var (
	InfoView = &view.View{
		Name:        "info",
		Description: "Titan node information",
		Measure:     TitanInfo,
		Aggregation: view.LastValue(),
		TagKeys:     []tag.Key{Version, Commit, NodeType},
	}

	APIRequestDurationView = &view.View{
		Measure:     APIRequestDuration,
		Aggregation: defaultMillisecondsDistribution,
		TagKeys:     []tag.Key{APIInterface, Endpoint},
	}
)

// DefaultViews is an array of OpenCensus views for metric gathering purposes
var DefaultViews = func() []*view.View {
	views := []*view.View{
		InfoView,
		APIRequestDurationView,
	}
	views = append(views, rpcmetrics.DefaultViews...)
	return views
}()

// SinceInMilliseconds returns the duration of time since the provide time as a float64.
func SinceInMilliseconds(startTime time.Time) float64 {
	return float64(time.Since(startTime).Nanoseconds()) / 1e6
}

// Timer is a function stopwatch, calling it starts the timer,
// calling the returned function will record the duration.
func Timer(ctx context.Context, m *stats.Float64Measure) func() {
	start := time.Now()
	return func() {
		stats.Record(ctx, m.M(SinceInMilliseconds(start)))
	}
}
