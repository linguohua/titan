package cache

import (
	"bytes"
	"encoding/gob"

	"github.com/linguohua/titan/api/types"
)

type EncodeCarfileCache struct {
	root                      string
	BlocksWaitList            []string
	BlocksDownloadSuccessList []string
	NextLayerCIDs             []string
	DownloadSources           []*types.DownloadSource
	TotalSize                 uint64
	DoneSize                  uint64
}

func encode(input interface{}) ([]byte, error) {
	var buffer bytes.Buffer
	enc := gob.NewEncoder(&buffer)
	err := enc.Encode(input)
	if err != nil {
		return nil, err
	}

	return buffer.Bytes(), nil
}

func decode(data []byte, out interface{}) error {
	buffer := bytes.NewBuffer(data)
	enc := gob.NewDecoder(buffer)
	err := enc.Decode(out)
	if err != nil {
		return err
	}
	return nil
}
