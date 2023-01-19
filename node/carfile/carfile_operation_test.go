package carfile

import (
	"testing"

	"github.com/linguohua/titan/node/carfile/carfilestore"
	"github.com/linguohua/titan/node/carfile/downloader"
)

func TestCarfileOperation(t *testing.T) {
	// carfileStore := blockstore.NewBlockStore("./blockstore", "FileStore")
	carfileStore := carfilestore.NewCarfileStore("./carfilestore", "FileStore")
	downloader := downloader.NewIPFS("http://192.168.0.132:5001", carfileStore)
	carfileOperation := NewCarfileOperation(nil, carfileStore, nil, downloader, nil)
	downloadOperation := &downloadOperation{downloader: downloader, carfileOperation: carfileOperation}
	// go carfileOperation.startCarfileDownloader()
	// t.Log("startCarfileDownloader")
	// time.Sleep(1 * time.Second)
	// _, err := carfileOperation.CacheCarfile(context.Background(), "QmPm6ZKzVAikwTnhfZCVBW91zi5FhmoH9XkQVVaqGX66nm", nil)
	// if err != nil {
	// 	t.Errorf("TestCarfileOperation error:%s", err.Error())
	// 	return
	// }
	// t.Log("CacheCarfile")
	// time.Sleep(2 * time.Minute)
	carfileCID := "QmUuNfFwuRrxbRFt5ze3EhuQgkGnutwZtsYMbAcYbtb6j3"
	carfile := &carfileCache{
		carfileCID:                carfileCID,
		blocksWaitList:            make([]string, 0),
		blocksDownloadSuccessList: make([]string, 0),
		nextLayerCIDs:             make([]string, 0),
		downloadSources:           nil,
		// waitListLock:              &sync.Mutex{},
	}

	layer := 1

	ret, err := carfile.downloadBlocksWithBreadthFirst([]string{carfile.carfileCID}, downloadOperation)
	if err != nil {
		t.Errorf("downloadBlocksWithBreadthFirst error:%s", err.Error())
		return
	}

	carfile.carfileSize = ret.linksSize + ret.downloadSize
	t.Logf("carfile size:%d", carfile.carfileSize)

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

	// carfile.downloadCarfile(carfileOperation)

}
