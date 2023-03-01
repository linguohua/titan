package carfile

import (
	"bytes"
	"encoding/gob"

	"github.com/linguohua/titan/api"
)

type EncodeCarfileCache struct {
	CarfileCID                string
	BlocksWaitList            []string
	BlocksDownloadSuccessList []string
	NextLayerCIDs             []string
	DownloadSources           []*api.DownloadSource
	CarfileSize               uint64
	DownloadSize              uint64
}

func encode(cfCache *carfileCache) ([]byte, error) {
	encodeCarfile := &EncodeCarfileCache{
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

func decode(carfileData []byte) (*carfileCache, error) {
	encodeCarfile := &EncodeCarfileCache{}

	buffer := bytes.NewBuffer(carfileData)
	enc := gob.NewDecoder(buffer)
	err := enc.Decode(encodeCarfile)
	if err != nil {
		return nil, err
	}
	cfCache := &carfileCache{}
	cfCache.carfileCID = encodeCarfile.CarfileCID
	cfCache.blocksWaitList = encodeCarfile.BlocksWaitList
	cfCache.blocksDownloadSuccessList = encodeCarfile.BlocksDownloadSuccessList
	cfCache.nextLayerCIDs = encodeCarfile.NextLayerCIDs
	cfCache.downloadSources = encodeCarfile.DownloadSources
	cfCache.carfileSize = encodeCarfile.CarfileSize
	cfCache.downloadSize = encodeCarfile.DownloadSize

	return cfCache, nil
}

func encodeWaitList(wl []*carfileCache) ([]byte, error) {
	waitList := make([]*EncodeCarfileCache, 0, len(wl))
	for _, cfCache := range wl {
		encodeCarfile := &EncodeCarfileCache{
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

func decodeWaitList(carfileData []byte) ([]*carfileCache, error) {
	var encodeCarfiles []*EncodeCarfileCache

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
