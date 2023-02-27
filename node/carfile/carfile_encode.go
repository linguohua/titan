package carfile

import (
	"bytes"
	"encoding/gob"

	"github.com/linguohua/titan/api"
)

type EncodeCarfile struct {
	CarfileCID                string
	BlocksWaitList            []string
	BlocksDownloadSuccessList []string
	NextLayerCIDs             []string
	DownloadSources           []*api.DownloadSource
	CarfileSize               uint64
	DownloadSize              uint64
}

func encodeCarfileCache(cfCache *carfileCache) ([]byte, error) {
	encodeCarfile := &EncodeCarfile{
		CarfileCID:                cfCache.carfileCID,
		BlocksWaitList:            cfCache.blocksWaitList,
		BlocksDownloadSuccessList: cfCache.blocksDownloadSuccessList,
		NextLayerCIDs:             cfCache.nextLayerCIDs,
		DownloadSources:           cfCache.downloadSources,
		CarfileSize:               cfCache.carfileSize,
		DownloadSize:              cfCache.downloadSize,
	}

	var buffer bytes.Buffer
	enc := gob.NewEncoder(&buffer)
	err := enc.Encode(encodeCarfile)
	if err != nil {
		return nil, err
	}

	return buffer.Bytes(), nil
}

func decodeCarfileCacheFromData(carfileData []byte, cfCache *carfileCache) error {
	encodeCarfile := &EncodeCarfile{}

	buffer := bytes.NewBuffer(carfileData)
	enc := gob.NewDecoder(buffer)
	err := enc.Decode(encodeCarfile)
	if err != nil {
		return err
	}

	cfCache.carfileCID = encodeCarfile.CarfileCID
	cfCache.blocksWaitList = encodeCarfile.BlocksWaitList
	cfCache.blocksDownloadSuccessList = encodeCarfile.BlocksDownloadSuccessList
	cfCache.nextLayerCIDs = encodeCarfile.NextLayerCIDs
	cfCache.downloadSources = encodeCarfile.DownloadSources
	cfCache.carfileSize = encodeCarfile.CarfileSize
	cfCache.downloadSize = encodeCarfile.DownloadSize

	return nil
}

func encodeWaitList(carfileCaches []*carfileCache) ([]byte, error) {
	waitList := make([]*EncodeCarfile, 0, len(carfileCaches))
	for _, cfCache := range carfileCaches {
		encodeCarfile := &EncodeCarfile{
			CarfileCID:                cfCache.carfileCID,
			BlocksWaitList:            cfCache.blocksWaitList,
			BlocksDownloadSuccessList: cfCache.blocksDownloadSuccessList,
			NextLayerCIDs:             cfCache.nextLayerCIDs,
			DownloadSources:           cfCache.downloadSources,
			CarfileSize:               cfCache.carfileSize,
			DownloadSize:              cfCache.downloadSize,
		}
		waitList = append(waitList, encodeCarfile)
	}

	var buffer bytes.Buffer
	enc := gob.NewEncoder(&buffer)
	err := enc.Encode(waitList)
	if err != nil {
		return nil, err
	}

	return buffer.Bytes(), nil
}

func decodeWaitListFromData(carfileData []byte) ([]*carfileCache, error) {
	var encodeCarfiles []*EncodeCarfile

	buffer := bytes.NewBuffer(carfileData)
	enc := gob.NewDecoder(buffer)
	err := enc.Decode(&encodeCarfiles)
	if err != nil {
		return nil, err
	}

	cfCaches := make([]*carfileCache, 0, len(encodeCarfiles))
	for _, encodeCarfile := range encodeCarfiles {
		cfCache := &carfileCache{}
		cfCache.carfileCID = encodeCarfile.CarfileCID
		cfCache.blocksWaitList = encodeCarfile.BlocksWaitList
		cfCache.blocksDownloadSuccessList = encodeCarfile.BlocksDownloadSuccessList
		cfCache.nextLayerCIDs = encodeCarfile.NextLayerCIDs
		cfCache.downloadSources = encodeCarfile.DownloadSources
		cfCache.carfileSize = encodeCarfile.CarfileSize
		cfCache.downloadSize = encodeCarfile.DownloadSize

		cfCaches = append(cfCaches, cfCache)
	}

	return cfCaches, nil
}
