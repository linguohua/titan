package helper

import (
	"time"

	"github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"
)

const (
	Batch = 5
	// number download block if failed
	BlockDownloadRetryNum = 1
	// Millisecond
	LoadBockTick = 10
	// validate timeout
	ValidateTimeout = 5
	// 3 seconds
	SchedulerApiTimeout = 3
	// seconds
	BlockDownloadTimeout = 15

	DownloadSrvPath          = "/block/get"
	DownloadTokenExpireAfter = 24 * time.Hour

	KeyFidPrefix     = "fid/"
	KeyCidPrefix     = "hash/"
	TcpPackMaxLength = 52428800
)

// type NodeParams struct {
// 	DS              datastore.Batching
// 	Scheduler       api.Scheduler
// 	CarfileStore    *carfilestore.CarfileStore
// 	DownloadSrvKey  string
// 	DownloadSrvAddr string
// 	IPFSAPI         string
// }

func CIDString2HashString(cidString string) (string, error) {
	cid, err := cid.Decode(cidString)
	if err != nil {
		return "", err
	}

	return cid.Hash().String(), nil
}

func HashString2CidString(hashString string) (string, error) {
	multihash, err := mh.FromHexString(hashString)
	if err != nil {
		return "", err
	}
	cid := cid.NewCidV1(cid.Raw, multihash)
	return cid.String(), nil
}
