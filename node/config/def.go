package config

import (
	"encoding"

	"os"
	"strconv"
	"time"
)

const (
	// RetrievalPricingDefault configures the node to use the default retrieval pricing policy.
	RetrievalPricingDefaultMode = "default"
	// RetrievalPricingExternal configures the node to use the external retrieval pricing script
	// configured by the user.
	RetrievalPricingExternalMode = "external"
)

// MaxTraversalLinks configures the maximum number of links to traverse in a DAG while calculating
// CommP and traversing a DAG with graphsync; invokes a budget on DAG depth and density.
var MaxTraversalLinks uint64 = 32 * (1 << 20)

func init() {
	if envMaxTraversal, err := strconv.ParseUint(os.Getenv("LOTUS_MAX_TRAVERSAL_LINKS"), 10, 64); err == nil {
		MaxTraversalLinks = envMaxTraversal
	}
}

// DefaultEdgeCfg returns the default edge config
func DefaultEdgeCfg() *EdgeCfg {
	return &EdgeCfg{
		ListenAddress:    "0.0.0.0:1234",
		Timeout:          "30s",
		CarfilestoreType: "FileStore",
		BandwidthUp:      1073741824,
		BandwidthDown:    1073741824,
		Locator:          true,
	}
}

// DefaultCandidateCfg returns the defualt candidate config
func DefaultCandidateCfg() *CandidateCfg {
	edgeCfg := EdgeCfg{
		ListenAddress:    "0.0.0.0:2345",
		Timeout:          "30s",
		CarfilestoreType: "FileStore",
		BandwidthUp:      1073741824,
		BandwidthDown:    1073741824,
		Locator:          true,
	}
	return &CandidateCfg{
		EdgeCfg:    edgeCfg,
		TcpSrvAddr: "0.0.0.0:9000",
		IpfsApiURL: "http://127.0.0.1:5001",
	}
}

var _ encoding.TextMarshaler = (*Duration)(nil)
var _ encoding.TextUnmarshaler = (*Duration)(nil)

// Duration is a wrapper type for time.Duration
// for decoding and encoding from/to TOML
type Duration time.Duration

// UnmarshalText implements interface for TOML decoding
func (dur *Duration) UnmarshalText(text []byte) error {
	d, err := time.ParseDuration(string(text))
	if err != nil {
		return err
	}
	*dur = Duration(d)
	return err
}

func (dur Duration) MarshalText() ([]byte, error) {
	d := time.Duration(dur)
	return []byte(d.String()), nil
}
