package carfile

import (
	"testing"

	"github.com/linguohua/titan/node/carfile/carfilestore"
	"github.com/linguohua/titan/node/carfile/downloader"
)

func TestCarfileOperation(t *testing.T) {
	carfileStore := carfilestore.NewCarfileStore("./carfilestore", "FileStore")
	downloader := downloader.NewIPFS("http://192.168.0.132:5001", carfileStore)
	carfileOperation := NewCarfileOperation(carfileStore, nil, downloader, nil)
	downloadOperation := &downloadOperation{downloader: downloader, carfileOperation: carfileOperation}

	carfileCID := "QmUuNfFwuRrxbRFt5ze3EhuQgkGnutwZtsYMbAcYbtb6j3"
	carfile := &carfileCache{
		carfileCID:                carfileCID,
		blocksWaitList:            make([]string, 0),
		blocksDownloadSuccessList: make([]string, 0),
		nextLayerCIDs:             make([]string, 0),
		downloadSources:           nil,
	}

	layer := 1

	ret, err := carfile.downloadBlocksWithBreadthFirst([]string{carfile.carfileCID}, downloadOperation)
	if err != nil {
		t.Errorf("downloadBlocksWithBreadthFirst error:%s", err.Error())
		return
	}

	carfile.carfileSize = ret.linksSize + ret.downloadSize
	t.Logf("storage size:%d", carfile.carfileSize)

	t.Logf("layer %d, cids:%v", layer, []string{carfile.carfileCID})

	layer++

	t.Logf("layer %d, cids len:%d, cids:%v", layer, len(ret.netLayerCids), ret.netLayerCids)

	for len(ret.netLayerCids) > 0 {
		ret, err = carfile.downloadBlocksWithBreadthFirst(ret.netLayerCids, downloadOperation)
		if err != nil {
			t.Errorf("downloadBlocksWithBreadthFirst error:%s", err.Error())
			return
		}

		layer++
		t.Logf("layer %d, cids len:%d", layer, len(ret.netLayerCids))
	}
}
