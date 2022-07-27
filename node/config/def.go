package config

import (
	"encoding"

	"os"
	"strconv"
	"time"

	"github.com/ipfs/go-cid"
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

func defCommon() Common {
	return Common{
		API: API{
			ListenAddress: "/ip4/127.0.0.1/tcp/1234/http",
			Timeout:       Duration(30 * time.Second),
		},
		Logging: Logging{
			SubsystemLevels: map[string]string{
				"example-subsystem": "INFO",
			},
		},
		Backup: Backup{
			DisableMetadataLog: true,
		},
		Libp2p: Libp2p{
			ListenAddresses: []string{
				"/ip4/0.0.0.0/tcp/0",
				"/ip6/::/tcp/0",
			},
			AnnounceAddresses:   []string{},
			NoAnnounceAddresses: []string{},

			ConnMgrLow:   150,
			ConnMgrHigh:  180,
			ConnMgrGrace: Duration(20 * time.Second),
		},
		Pubsub: Pubsub{
			Bootstrapper: false,
			DirectPeers:  nil,
		},
	}

}

var DefaultSimultaneousTransfers = uint64(20)

// DefaultFullNode returns the default config
func DefaultFullNode() *FullNode {
	return &FullNode{
		Common: defCommon(),
		Client: Client{
			SimultaneousTransfersForStorage:   DefaultSimultaneousTransfers,
			SimultaneousTransfersForRetrieval: DefaultSimultaneousTransfers,
		},
		Chainstore: Chainstore{
			EnableSplitstore: false,
			Splitstore: Splitstore{
				ColdStoreType: "universal",
				HotStoreType:  "badger",
				MarkSetType:   "badger",

				HotStoreFullGCFrequency: 20,
			},
		},
	}
}

func DefaultStorageMiner() *StorageMiner {
	cfg := &StorageMiner{
		Common: defCommon(),

		Sealing: SealingConfig{
			MaxWaitDealsSectors:       2, // 64G with 32G sectors
			MaxSealingSectors:         0,
			MaxSealingSectorsForDeals: 0,
			WaitDealsDelay:            Duration(time.Hour * 6),
			AlwaysKeepUnsealedCopy:    true,
			FinalizeEarly:             false,
			MakeNewSectorForDeals:     true,

			CollateralFromMinerBalance: false,
			DisableCollateralFallback:  false,

			BatchPreCommits: true,

			PreCommitBatchWait: Duration(24 * time.Hour), // this should be less than 31.5 hours, which is the expiration of a precommit ticket
			// XXX snap deals wait deals slack if first
			PreCommitBatchSlack: Duration(3 * time.Hour), // time buffer for forceful batch submission before sectors/deals in batch would start expiring, higher value will lower the chances for message fail due to expiration

			AggregateCommits: true,
			CommitBatchWait:  Duration(24 * time.Hour), // this can be up to 30 days
			CommitBatchSlack: Duration(1 * time.Hour),  // time buffer for forceful batch submission before sectors/deals in batch would start expiring, higher value will lower the chances for message fail due to expiration

			TerminateBatchMin:  1,
			TerminateBatchMax:  100,
			TerminateBatchWait: Duration(5 * time.Minute),
		},

		Proving: ProvingConfig{
			ParallelCheckLimit: 128,
		},

		Storage: SealerConfig{
			AllowAddPiece:            true,
			AllowPreCommit1:          true,
			AllowPreCommit2:          true,
			AllowCommit:              true,
			AllowUnseal:              true,
			AllowReplicaUpdate:       true,
			AllowProveReplicaUpdate2: true,
			AllowRegenSectorKey:      true,

			// Default to 10 - tcp should still be able to figure this out, and
			// it's the ratio between 10gbit / 1gbit
			ParallelFetchLimit: 10,
		},

		Dealmaking: DealmakingConfig{
			ConsiderOnlineStorageDeals:     true,
			ConsiderOfflineStorageDeals:    true,
			ConsiderOnlineRetrievalDeals:   true,
			ConsiderOfflineRetrievalDeals:  true,
			ConsiderVerifiedStorageDeals:   true,
			ConsiderUnverifiedStorageDeals: true,
			PieceCidBlocklist:              []cid.Cid{},
			// TODO: It'd be nice to set this based on sector size
			MaxDealStartDelay:               Duration(time.Hour * 24 * 14),
			ExpectedSealDuration:            Duration(time.Hour * 24),
			PublishMsgPeriod:                Duration(time.Hour),
			MaxDealsPerPublishMsg:           8,
			MaxProviderCollateralMultiplier: 2,

			SimultaneousTransfersForStorage:          DefaultSimultaneousTransfers,
			SimultaneousTransfersForStoragePerClient: 0,
			SimultaneousTransfersForRetrieval:        DefaultSimultaneousTransfers,

			StartEpochSealingBuffer: 480, // 480 epochs buffer == 4 hours from adding deal to sector to sector being sealed

			RetrievalPricing: &RetrievalPricing{
				Strategy: RetrievalPricingDefaultMode,
				Default: &RetrievalPricingDefault{
					VerifiedDealsFreeTransfer: true,
				},
				External: &RetrievalPricingExternal{
					Path: "",
				},
			},
		},

		IndexProvider: IndexProviderConfig{
			Enable:               true,
			EntriesCacheCapacity: 1024,
			EntriesChunkSize:     16384,
			// The default empty TopicName means it is inferred from network name, in the following
			// format: "/indexer/ingest/<network-name>"
			TopicName:         "",
			PurgeCacheOnStart: false,
		},

		Subsystems: MinerSubsystemConfig{
			EnableMining:        true,
			EnableSealing:       true,
			EnableSectorStorage: true,
			EnableMarkets:       true,
		},

		Addresses: MinerAddressConfig{
			PreCommitControl:   []string{},
			CommitControl:      []string{},
			TerminateControl:   []string{},
			DealPublishControl: []string{},
		},

		DAGStore: DAGStoreConfig{
			MaxConcurrentIndex:         5,
			MaxConcurrencyStorageCalls: 100,
			MaxConcurrentUnseals:       5,
			GCInterval:                 Duration(1 * time.Minute),
		},
	}

	cfg.Common.API.ListenAddress = "/ip4/127.0.0.1/tcp/2345/http"
	cfg.Common.API.RemoteListenAddress = "127.0.0.1:2345"
	return cfg
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
