package block

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"sync"
	"time"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/linguohua/titan/node/helper"
)

type IPFS struct {
}

type ipfsBlock struct {
	cid cid.Cid
	raw []byte
	err error
}

func (ipfs *IPFS) loadBlocks(block *Block, req []*delayReq) {
	loadBlocksFromIPFS(block, req)
}

// curl "http://127.0.0.1:8080/ipfs/bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi?format=raw" > cat.jpg
func getBlockFromIPFSGateway(wg *sync.WaitGroup, block *ipfsBlock, url string) {
	defer wg.Done()

	// log.Infof("getBlockFromIPFSGateway url:%s", url)

	client := &http.Client{Timeout: 15 * time.Second}
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		block.err = err
		return
	}

	resp, err := client.Do(req)
	if err != nil {
		block.err = err
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		block.err = fmt.Errorf("resp.StatusCode != 200 ")
		return
	}

	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		block.err = err
		return
	}
	block.raw = data
}

func getBlocksWithHttp(block *Block, cids []cid.Cid) ([]blocks.Block, error) {
	ipfsbs := make([]*ipfsBlock, 0, len(cids))

	var wg sync.WaitGroup

	for _, cid := range cids {
		var url = fmt.Sprintf("%s/%s?format=raw", block.ipfsGateway, cid.String())
		var ipfsb = &ipfsBlock{cid: cid, err: nil, raw: nil}
		ipfsbs = append(ipfsbs, ipfsb)

		wg.Add(1)
		go getBlockFromIPFSGateway(&wg, ipfsb, url)
	}

	wg.Wait()

	bs := make([]blocks.Block, 0, len(ipfsbs))
	for _, b := range ipfsbs {
		if b.err != nil {
			log.Errorf("get ipfs block error:%s", b.err.Error())
			continue
		}

		if b.raw == nil {
			log.Errorf("b.raw == nil")
			continue
		}

		block, err := blocks.NewBlockWithCid(b.raw, b.cid)
		if err != nil {
			log.Errorf("getBlocksWithHttp error:%s", err)
			continue
		}

		bs = append(bs, block)
	}

	log.Infof("getBlocksWithHttp block len:%d", len(bs))
	return bs, nil
}

func loadBlocksAsync(block *Block, cids []cid.Cid) ([]blocks.Block, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	blockCh, err := block.exchange.GetBlocks(ctx, cids)
	if err != nil {
		log.Errorf("CacheData, loadBlock error:", err)
		return nil, err
	}

	results := make([]blocks.Block, 0, len(cids))
	for block := range blockCh {
		results = append(results, block)
	}
	// log.Debug("get block end")
	return results, nil
}

func loadBlocksFromIPFS(block *Block, req []*delayReq) {
	req = block.filterAvailableReq(req)
	ctx := context.Background()

	cids := make([]cid.Cid, 0, len(req))
	reqMap := make(map[string]*delayReq)
	for _, reqData := range req {
		target, err := cid.Decode(reqData.blockInfo.Cid)
		if err != nil {
			log.Errorf("loadBlocksAsync failed to decode CID %v", err)
			continue
		}

		// // convert cid to v0
		// if target.Version() != 0 && target.Type() == cid.DagProtobuf {
		// 	target = cid.NewCidV0(target.Hash())
		// }

		cids = append(cids, target)
		reqMap[reqData.blockInfo.Cid] = reqData
	}

	if len(cids) == 0 {
		log.Debug("loadBlocksAsync, len(cids) == 0")
		return
	}

	blocks, err := getBlocksWithHttp(block, cids)
	if err != nil {
		log.Errorf("loadBlocksAsync loadBlocks err %v", err)
		return
	}

	for _, b := range blocks {
		cidStr := b.Cid().String()
		req, ok := reqMap[cidStr]
		if !ok {
			log.Errorf("loadBlocksFromIPFS cid %s not in map", cidStr)
			continue
		}

		err = block.saveBlock(ctx, b.RawData(), req.blockInfo.Cid, req.blockInfo.Fid)
		if err != nil {
			log.Errorf("loadBlocksFromIPFS save block error:%s", err.Error())
			continue
		}

		// get block links
		links, err := block.resolveLinks(b)
		if err != nil {
			log.Errorf("loadBlocksFromIPFS resolveLinks error:%s", err.Error())
			continue
		}

		linksSize := uint64(0)
		cids := make([]string, 0, len(links))
		for _, link := range links {
			cids = append(cids, link.Cid.String())
			linksSize += link.Size
		}

		bStat := blockStat{cid: cidStr, fid: req.blockInfo.Fid, links: cids, blockSize: len(b.RawData()), linksSize: linksSize, carFileCid: req.carFileCid, CacheID: req.CacheID}
		block.cacheResult(ctx, nil, bStat)

		log.Infof("cache data,cid:%s,err:%v", cidStr, err)

		delete(reqMap, cidStr)
	}

	if len(reqMap) > 0 {
		err = fmt.Errorf("Request timeout")
		for _, v := range reqMap {
			if v.count > helper.MaxReqCount {
				block.cacheResultWithError(ctx, blockStat{cid: v.blockInfo.Cid, fid: v.blockInfo.Fid, carFileCid: v.carFileCid, CacheID: v.CacheID}, err)
				log.Infof("cache data faile, cid:%s, count:%d", v.blockInfo.Cid, v.count)
			} else {
				v.count++
				block.addReq2WaitList([]*delayReq{v})
			}
		}
	}
}
