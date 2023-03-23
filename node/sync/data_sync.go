package sync

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"hash/fnv"

	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/linguohua/titan/node/carfile/store"
	mh "github.com/multiformats/go-multihash"
)

var log = logging.Logger("datasync")

type DataSync struct {
	carfileStore *store.CarfileStore
	cacher       Cacher
}

type Cacher interface {
	CacheCarForSyncData(carfileHash []string) error
}

func NewDataSync(carfileStore *store.CarfileStore, cacher Cacher) *DataSync {
	return &DataSync{carfileStore: carfileStore, cacher: cacher}
}

func (ds *DataSync) CompareChecksums(ctx context.Context, bucketCount uint32, checksums map[uint32]string) (mismatchKey []uint32, err error) {
	hashes, err := ds.carfileStore.CarfileHashes()
	if err != nil {
		return nil, err
	}

	multihashes := ds.multihashSort(hashes, bucketCount)
	css, err := ds.caculateChecksums(multihashes)
	if err != nil {
		return nil, err
	}

	for k, v := range checksums {
		cs := css[k]
		if cs != v {
			mismatchKey = append(mismatchKey, k)
		}
		delete(multihashes, k)
	}

	// Remove the redundant carfile
	for _, v := range multihashes {
		err := ds.removeCarfiles(v)
		if err != nil {
			log.Errorf("remove carfiles error %s", err.Error())
		}
	}

	return mismatchKey, nil
}

func (ds *DataSync) multihashSort(hashes []string, bucketCount uint32) map[uint32][]string {
	multihashes := make(map[uint32][]string)
	// appen carfilehash by hash code
	for _, hash := range hashes {
		multihash, err := mh.FromHexString(hash)
		if err != nil {
			log.Errorf("decode multihash error:%s", err.Error())
			continue
		}

		h := fnv.New32a()
		h.Write(multihash)
		k := h.Sum32() % bucketCount

		multihashes[k] = append(multihashes[k], hash)

	}

	return multihashes
}

func (ds *DataSync) caculateChecksums(multihashes map[uint32][]string) (map[uint32]string, error) {
	checksums := make(map[uint32]string)
	for k, v := range multihashes {
		checksum, err := ds.caculateChecksum(v)
		if err != nil {
			return nil, err
		}

		checksums[k] = checksum
	}
	return checksums, nil
}

func (ds *DataSync) caculateChecksum(carfileHashes []string) (string, error) {
	hash := sha256.New()

	for _, h := range carfileHashes {
		data := []byte(h)
		_, err := hash.Write(data)
		if err != nil {
			return "", err
		}
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func (ds *DataSync) removeCarfiles(carfiles []string) error {
	for _, hash := range carfiles {
		multihash, err := mh.FromHexString(hash)
		if err != nil {
			return err
		}

		cid := cid.NewCidV1(cid.Raw, multihash)
		err = ds.carfileStore.DeleteCarfile(cid)
		if err != nil {
			return err
		}
	}

	return nil
}

func (ds *DataSync) CompareCarfiles(ctx context.Context, bucketCount uint32, multiHashes map[uint32][]string) error {
	hashes, err := ds.carfileStore.CarfileHashes()
	if err != nil {
		return err
	}

	needToDownloadCarfiles := make([]string, 0)
	needToDeleteCarfiles := make([]string, 0)
	mhs := ds.multihashSort(hashes, bucketCount)

	for k, v := range multiHashes {
		mh, ok := mhs[k]
		if !ok {
			needToDownloadCarfiles = append(needToDownloadCarfiles, v...)
			continue
		}

		r, l := ds.compareCarfiles(v, mh)
		if len(r) > 0 {
			needToDeleteCarfiles = append(needToDeleteCarfiles, r...)
		}
		if len(l) > 0 {
			needToDownloadCarfiles = append(needToDownloadCarfiles, l...)
		}
	}

	go ds.cacher.CacheCarForSyncData(needToDownloadCarfiles)

	return ds.removeCarfiles(needToDeleteCarfiles)
}

func (ds *DataSync) compareCarfiles(dest []string, src []string) (redundant, lack []string) {
	destMap := make(map[string]struct{})

	for _, v := range dest {
		destMap[v] = struct{}{}
	}

	for _, v := range src {
		if _, ok := destMap[v]; !ok {
			redundant = append(redundant, v)
			continue
		}

		delete(destMap, v)
	}

	for k := range destMap {
		lack = append(lack, k)
	}

	return
}
