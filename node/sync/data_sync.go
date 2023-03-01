package sync

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"

	logging "github.com/ipfs/go-log/v2"
	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/node/carfile/carfilestore"
)

var log = logging.Logger("datasync")

const maxCarfileCount = 2000000

type DataSync struct {
	carfileStore *carfilestore.CarfileStore
	carfileMap   map[string]struct{}
	isChecking   bool
}

func NewDataSync(carfileStore *carfilestore.CarfileStore) *DataSync {
	return &DataSync{carfileStore: carfileStore}
}

func (ds *DataSync) CompareChecksum(ctx context.Context, succeededCarfilesChecksum, unsucceededCarfilesChecksum string) (*api.CompareResult, error) {
	completeCarfileHashes, err := ds.carfileStore.GetCompleteCarfileHashList()
	if err != nil {
		return nil, err
	}

	incompleteCarfileHashes, err := ds.carfileStore.GetIncompleteCarfileHashList()
	if err != nil {
		return nil, err
	}

	completeCarfilesChecksum, err := ds.caculateChecksum(completeCarfileHashes)
	if err != nil {
		return nil, err
	}

	incompleteCarfilesChecksum, err := ds.caculateChecksum(incompleteCarfileHashes)
	if err != nil {
		return nil, err
	}

	return &api.CompareResult{IsSusseedCarfilesOk: completeCarfilesChecksum == succeededCarfilesChecksum, IsUnsusseedCarfilesOk: incompleteCarfilesChecksum == unsucceededCarfilesChecksum}, nil
}

func (ds *DataSync) BeginCheckCarfiles(ctx context.Context) error {
	if ds.isChecking {
		return fmt.Errorf("checking carfile now, can not do again")
	}
	// clean
	ds.carfileMap = make(map[string]struct{}, 0)
	return nil
}

func (ds *DataSync) PrepareCarfiles(ctx context.Context, carfileHashes []string) error {
	if ds.isChecking {
		return fmt.Errorf("checking carfile now, can not do again")
	}

	total := len(ds.carfileMap) + len(carfileHashes)
	if total > maxCarfileCount {
		return fmt.Errorf("total carfile is out of %d", maxCarfileCount)
	}

	for _, hash := range carfileHashes {
		ds.carfileMap[hash] = struct{}{}
	}

	return nil
}

func (ds *DataSync) DoCheckCarfiles(ctx context.Context, carfilesChecksum string, isSusseededCarfiles bool) error {
	ok, err := ds.isPrepare(carfilesChecksum)
	if err != nil {
		return err
	}

	if !ok {
		return fmt.Errorf("checksum not match prepare carfiles")
	}

	ds.isChecking = true
	defer func() {
		ds.isChecking = false
		ds.carfileMap = make(map[string]struct{}, 0)
	}()

	localCarfileMap, err := ds.localCarfilesToMap(isSusseededCarfiles)
	if err != nil {
		return err
	}
	return ds.checkCarfiles(localCarfileMap, isSusseededCarfiles)
}

func (ds *DataSync) isPrepare(carfilesChecksum string) (bool, error) {
	carfileHashes := make([]string, 0, len(ds.carfileMap))
	for hash := range ds.carfileMap {
		carfileHashes = append(carfileHashes, hash)
	}

	sort.Strings(carfileHashes)

	checksum, err := ds.caculateChecksum(carfileHashes)
	if err != nil {
		return false, err
	}

	return carfilesChecksum == checksum, nil
}

func (ds *DataSync) caculateChecksum(carfileHashes []string) (string, error) {
	hash := sha256.New()

	for _, ch := range carfileHashes {
		data := []byte(ch)
		_, err := hash.Write(data)
		if err != nil {
			return "", err
		}
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func (ds *DataSync) localCarfilesToMap(isSusseededCarfiles bool) (map[string]struct{}, error) {
	var err error
	var carfileHashes []string
	if isSusseededCarfiles {
		carfileHashes, err = ds.carfileStore.GetCompleteCarfileHashList()
	} else {
		carfileHashes, err = ds.carfileStore.GetIncompleteCarfileHashList()
	}

	if err != nil {
		return nil, err
	}

	carfileMap := make(map[string]struct{})
	for _, hash := range carfileHashes {
		carfileMap[hash] = struct{}{}
	}

	return carfileMap, nil
}

func (ds *DataSync) checkCarfiles(localCarfile map[string]struct{}, isSucceededCarfiles bool) error {
	for hash := range ds.carfileMap {
		_, ok := localCarfile[hash]
		if ok {
			delete(localCarfile, hash)
			delete(ds.carfileMap, hash)
		}
	}

	needToDownloadCarfiles := ds.carfileMap
	needToDeleteCarfiles := localCarfile

	_ = needToDownloadCarfiles
	_ = needToDeleteCarfiles

	// 如果是失败的carfile, 不用下载，只要删除多余的就可以

	return nil
}
