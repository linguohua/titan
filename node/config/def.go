package config

import (
	"encoding"
	"os"
	"strconv"
	"time"

	"github.com/google/uuid"
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
	if envMaxTraversal, err := strconv.ParseUint(os.Getenv("TITAN_MAX_TRAVERSAL_LINKS"), 10, 64); err == nil {
		MaxTraversalLinks = envMaxTraversal
	}
}

// DefaultEdgeCfg returns the default edge config
func DefaultEdgeCfg() *EdgeCfg {
	return &EdgeCfg{
		ListenAddress:    "0.0.0.0:1234",
		Timeout:          "30s",
		CarfileStorePath: "",
		BandwidthUp:      104857600,
		BandwidthDown:    1073741824,
		Locator:          true,

		CertificatePath:    "",
		PrivateKeyPath:     "",
		CaCertificatePath:  "",
		InsecureSkipVerify: true,

		FetchBlockTimeout: 15,
		FetchBlockRetry:   1,
		FetchBatch:        5,
	}
}

// DefaultCandidateCfg returns the default candidate config
func DefaultCandidateCfg() *CandidateCfg {
	edgeCfg := EdgeCfg{
		ListenAddress:    "0.0.0.0:2345",
		Timeout:          "30s",
		CarfileStorePath: "",
		BandwidthUp:      1073741824,
		BandwidthDown:    1073741824,
		Locator:          true,

		InsecureSkipVerify: true,
		CertificatePath:    "",
		PrivateKeyPath:     "",
		CaCertificatePath:  "",

		FetchBlockTimeout: 15,
		FetchBlockRetry:   1,
		FetchBatch:        5,
	}
	return &CandidateCfg{
		EdgeCfg:    edgeCfg,
		TCPSrvAddr: "0.0.0.0:9000",
		IpfsAPIURL: "http://127.0.0.1:5001",
	}
}

func DefaultLocatorCfg() *LocatorCfg {
	return &LocatorCfg{
		ListenAddress:   "0.0.0.0:5000",
		Timeout:         "30s",
		GeoDBPath:       "./city.mmdb",
		DatabaseAddress: "user01:sql001@tcp(127.0.0.1:3306)/locator",
		UUID:            uuid.NewString(),

		InsecureSkipVerify: true,
		CertificatePath:    "",
		PrivateKeyPath:     "",
		CaCertificatePath:  "",
	}
}

func DefaultSchedulerCfg() *SchedulerCfg {
	return &SchedulerCfg{
		RPCURL:             "https://localhost:3456/rpc/v0",
		ListenAddress:      "0.0.0.0:3456",
		InsecureSkipVerify: true,
		CertificatePath:    "",
		PrivateKeyPath:     "",
		CaCertificatePath:  "",
		AreaID:             "CN-GD-Shenzhen",
		DatabaseAddress:    "user01:sql001@tcp(127.0.0.1:3306)/titan",
		EnableValidation:   true,
		EtcdAddresses:      []string{"192.168.0.160:2379"},
		CandidateReplicas:  0,
		ValidatorRatio:     1,
		ValidatorBaseBwDn:  100,
	}
}

var (
	_ encoding.TextMarshaler   = (*Duration)(nil)
	_ encoding.TextUnmarshaler = (*Duration)(nil)
)

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
