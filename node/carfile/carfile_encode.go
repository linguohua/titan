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
	DownloadSources           []*api.DowloadSource
	CarfileSize               uint64
	DownloadSize              uint64
}

func ecodeCarfile(cf *carfile) ([]byte, error) {
	encodeCarfile := &EncodeCarfile{
		CarfileCID:                cf.carfileCID,
		BlocksWaitList:            cf.blocksWaitList,
		BlocksDownloadSuccessList: cf.blocksDownloadSuccessList,
		NextLayerCIDs:             cf.nextLayerCIDs,
		DownloadSources:           cf.downloadSources,
		CarfileSize:               cf.carfileSize,
		DownloadSize:              cf.downloadSize}

	var buffer bytes.Buffer
	enc := gob.NewEncoder(&buffer)
	err := enc.Encode(encodeCarfile)
	if err != nil {
		return nil, err
	}

	return buffer.Bytes(), nil
}

func decodeCarfileFromData(carfileData []byte, cf *carfile) error {
	encodeCarfile := &EncodeCarfile{}

	buffer := bytes.NewBuffer(carfileData)
	enc := gob.NewDecoder(buffer)
	err := enc.Decode(encodeCarfile)
	if err != nil {
		return err
	}

	cf.carfileCID = encodeCarfile.CarfileCID
	cf.blocksWaitList = encodeCarfile.BlocksWaitList
	cf.blocksDownloadSuccessList = encodeCarfile.BlocksDownloadSuccessList
	cf.nextLayerCIDs = encodeCarfile.NextLayerCIDs
	cf.downloadSources = encodeCarfile.DownloadSources
	cf.carfileSize = encodeCarfile.CarfileSize
	cf.downloadSize = encodeCarfile.DownloadSize

	return nil
}

func ecodeWaitList(carfiles []*carfile) ([]byte, error) {
	waitList := make([]*EncodeCarfile, 0, len(carfiles))
	for _, cf := range carfiles {
		encodeCarfile := &EncodeCarfile{
			CarfileCID:                cf.carfileCID,
			BlocksWaitList:            cf.blocksWaitList,
			BlocksDownloadSuccessList: cf.blocksDownloadSuccessList,
			NextLayerCIDs:             cf.nextLayerCIDs,
			DownloadSources:           cf.downloadSources,
			CarfileSize:               cf.carfileSize,
			DownloadSize:              cf.downloadSize}
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

func decodeWaitListFromData(carfileData []byte) ([]*carfile, error) {
	var encodeCarfiles []*EncodeCarfile

	buffer := bytes.NewBuffer(carfileData)
	enc := gob.NewDecoder(buffer)
	err := enc.Decode(&encodeCarfiles)
	if err != nil {
		return nil, err
	}

	cfs := make([]*carfile, 0, len(encodeCarfiles))
	for _, encodeCarfile := range encodeCarfiles {
		cf := &carfile{}
		cf.carfileCID = encodeCarfile.CarfileCID
		cf.blocksWaitList = encodeCarfile.BlocksWaitList
		cf.blocksDownloadSuccessList = encodeCarfile.BlocksDownloadSuccessList
		cf.nextLayerCIDs = encodeCarfile.NextLayerCIDs
		cf.downloadSources = encodeCarfile.DownloadSources
		cf.carfileSize = encodeCarfile.CarfileSize
		cf.downloadSize = encodeCarfile.DownloadSize

		cfs = append(cfs, cf)
		log.Infof("encodeCarfile %v", encodeCarfile)
	}

	return cfs, nil
}
