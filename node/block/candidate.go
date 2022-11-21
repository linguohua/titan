package block

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/linguohua/titan/node/helper"
)

type Candidate struct {
	deviceID   string
	downSrvURL string
	token      string
}

func (candidate *Candidate) loadBlocks(block *Block, req []*delayReq) {
	loadBlocksFromCandidate(block, req)
}

func getBlockFromCandidate(url string, tk string) ([]byte, error) {
	client := &http.Client{Timeout: helper.BlockDownloadTimeout * time.Second}
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Token", tk)
	req.Header.Set("App-Name", "edge")

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	return ioutil.ReadAll(resp.Body)
}

func loadBlocksFromCandidate(block *Block, reqs []*delayReq) {
	reqs = block.filterAvailableReq(reqs)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for _, req := range reqs {
		if len(req.downloadURL) == 0 {
			log.Errorf("loadBlocksFromCandidate req downloadURL is nil")
			continue
		}

		url := fmt.Sprintf("%s?cid=%s", req.downloadURL, req.blockInfo.Cid)
		data, err := getBlockFromCandidate(url, req.downloadToken)
		if err != nil {
			log.Errorf("loadBlocksFromCandidate get block from candidate error:%s", err.Error())
			block.cacheResultWithError(ctx, blockStat{cid: req.blockInfo.Cid, carFileCid: req.carFileCid, CacheID: req.CacheID}, err)
			continue
		}

		err = block.saveBlock(ctx, data, req.blockInfo.Cid, fmt.Sprintf("%d", req.blockInfo.Fid))
		if err != nil {
			log.Errorf("loadBlocksFromCandidate save block error:%s", err.Error())
			block.cacheResultWithError(ctx, blockStat{cid: req.blockInfo.Cid, carFileCid: req.carFileCid, CacheID: req.CacheID}, err)
			continue
		}

		links, err := getLinks(block, data, req.blockInfo.Cid)
		if err != nil {
			log.Errorf("loadBlocksFromCandidate resolveLinks error:%s", err.Error())
			block.cacheResultWithError(ctx, blockStat{cid: req.blockInfo.Cid, carFileCid: req.carFileCid, CacheID: req.CacheID}, err)
			continue
		}

		linksSize := uint64(0)
		cids := make([]string, 0, len(links))
		for _, link := range links {
			cids = append(cids, link.Cid.String())
			linksSize += link.Size
		}

		bInfo := blockStat{cid: req.blockInfo.Cid, links: cids, blockSize: len(data), linksSize: linksSize, carFileCid: req.carFileCid, CacheID: req.CacheID}
		block.cacheResult(ctx, nil, bInfo)

		log.Infof("loadBlocksFromCandidate, cid:%s,err:%v", req.blockInfo.Cid, err)
	}
}
