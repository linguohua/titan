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
	BlocksDownloadFailedList  []string
	DownloadSources           []*api.DowloadSource
	CarfileSize               int64
	DownloadSize              int64
}

func ecodeCarfile(cf *carfile) ([]byte, error) {
	encodeCarfile := &EncodeCarfile{
		CarfileCID:                cf.carfileCID,
		BlocksWaitList:            cf.blocksWaitList,
		BlocksDownloadSuccessList: cf.blocksDownloadSuccessList,
		BlocksDownloadFailedList:  cf.blocksDownloadFailedList,
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

func decodeCarfileFromBuffer(carfileData []byte, cf *carfile) error {
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
	cf.blocksDownloadFailedList = encodeCarfile.BlocksDownloadFailedList
	cf.downloadSources = encodeCarfile.DownloadSources
	cf.carfileSize = encodeCarfile.CarfileSize
	cf.downloadSize = encodeCarfile.DownloadSize

	return nil
}
