package sync

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	logging "github.com/ipfs/go-log/v2"
	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/node/carfile/carfilestore"
)

var log = logging.Logger("datasync")

type DataSync struct {
	carfileStore                *carfilestore.CarfileStore
	carfileMap                  map[string]struct{}
	checkID                     string
	isCheckingCompleteCarfile   bool
	isCheckingIncompleteCarfile bool
}

func NewDataSync(carfileStore *carfilestore.CarfileStore) *DataSync {
	return &DataSync{carfileStore: carfileStore}
}

func (ds *DataSync) CheckSummary(ctx context.Context, susseedCarfilesHash, unsusseedCarfilesHash string) (*api.CheckSummaryResult, error) {
	completeCarfileHashList, err := ds.carfileStore.GetCompleteCarfileHashList()
	if err != nil {
		return nil, err
	}

	incompleteCarfileHashList, err := ds.carfileStore.GetIncompleteCarfileHashList()
	if err != nil {
		return nil, err
	}

	var mergeCompleteCarfileHash string
	for _, completeCarfileHash := range completeCarfileHashList {
		mergeCompleteCarfileHash += completeCarfileHash
	}

	var mergeIncompleteCarfilesHash string
	for _, incompleteCarfileHash := range incompleteCarfileHashList {
		mergeIncompleteCarfilesHash += incompleteCarfileHash
	}

	hash := sha256.New()
	hash.Write([]byte(mergeCompleteCarfileHash))
	completeCarfilesHash := hex.EncodeToString(hash.Sum(nil))

	hash.Reset()
	hash.Write([]byte(mergeIncompleteCarfilesHash))
	incompleteCarfilesHash := hex.EncodeToString(hash.Sum(nil))

	return &api.CheckSummaryResult{IsSusseedCarfilesOk: completeCarfilesHash == susseedCarfilesHash, IsUnsusseedCarfilesOk: incompleteCarfilesHash == unsusseedCarfilesHash}, nil
}

func (ds *DataSync) PrepareCheckCarfiles(carfileHashList []string, isSucceededCarfileList bool, checkID string) error {
	if ds.isCheckingCompleteCarfile && isSucceededCarfileList {
		return fmt.Errorf("It is checking complete carfile")
	}

	if ds.isCheckingIncompleteCarfile && !isSucceededCarfileList {
		return fmt.Errorf("It is checking incomplete carfile")
	}
	// clean carfielMap if checkID not match
	if ds.checkID != checkID {
		ds.checkID = checkID
		ds.carfileMap = make(map[string]struct{})
	}

	for _, carfileHash := range carfileHashList {
		ds.carfileMap[carfileHash] = struct{}{}
	}
	return nil
}

func (ds *DataSync) CheckCarfiles(isSucceededCarfileList bool, checkID string) error {
	if ds.checkID != checkID {
		ds.carfileMap = make(map[string]struct{})
		return fmt.Errorf("prepare checkID is %s, but now checkID == %s", ds.checkID, checkID)
	}

	carfileMap := ds.carfileMap
	ds.carfileMap = make(map[string]struct{})

	if isSucceededCarfileList {
		go ds.checkCompleteCarfiles(carfileMap)
	} else {
		go ds.checkIncompleteCarfiles(carfileMap)
	}
	return nil
}

func (ds *DataSync) checkCompleteCarfiles(carfileMap map[string]struct{}) error {
	ds.isCheckingCompleteCarfile = true
	defer func() {
		ds.isCheckingCompleteCarfile = false
	}()

	// TODO: implement check
	return nil
}

func (ds *DataSync) checkIncompleteCarfiles(carfileMap map[string]struct{}) error {
	ds.isCheckingIncompleteCarfile = true
	defer func() {
		ds.isCheckingIncompleteCarfile = false
	}()

	// TODO: implement check
	return nil
}
