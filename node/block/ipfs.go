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

	client := &http.Client{Timeout: 5 * time.Second}
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
		url := fmt.Sprintf("%s/%s?format=raw", block.ipfsGateway, cid.String())
		wg.Add(1)
		ipfsb := &ipfsBlock{cid: cid, err: nil, raw: nil}
		ipfsbs = append(ipfsbs, ipfsb)
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
		target, err := cid.Decode(reqData.cid)
		if err != nil {
			log.Errorf("loadBlocksAsync failed to decode CID %v", err)
			continue
		}

		// // convert cid to v0
		// if target.Version() != 0 && target.Type() == cid.DagProtobuf {
		// 	target = cid.NewCidV0(target.Hash())
		// }

		cids = append(cids, target)
		reqMap[reqData.cid] = reqData
	}

	if len(cids) == 0 {
		log.Debug("loadBlocksAsync, len(cids) == 0")
		return
	}

	var blocks []blocks.Block
	var err error
	if len(block.ipfsGateway) > 0 {
		blocks, err = getBlocksWithHttp(block, cids)
	} else {
		blocks, err = loadBlocksAsync(block, cids)
	}
	if err != nil {
		log.Errorf("loadBlocksAsync loadBlocks err %v", err)
		return
	}

	var from = ""
	for _, b := range blocks {
		cidStr := b.Cid().String()
		err = block.blockStore.Put(cidStr, b.RawData())
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
		log.Infof("start caceh result b == nil %v", b == nil)
		req := reqMap[cidStr]
		log.Infof("start caceh result2 b == nil %v", b == nil)
		bInfo := blockInfo{cid: cidStr, links: cids, blockSize: len(b.RawData()), linksSize: linksSize, carFileCid: req.carFileCid}
		block.cacheResult(ctx, from, nil, bInfo)

		log.Infof("cache data,cid:%s,err:%v", cidStr, err)

		delete(reqMap, cidStr)
	}

	if len(reqMap) > 0 {
		err = fmt.Errorf("Request timeout")
		for _, v := range reqMap {
			if v.count > helper.MaxReqCount {
				block.cacheResultWithError(ctx, v.cid, err)
				log.Infof("cache data faile, cid:%s, count:%d", v.cid, v.count)
			} else {
				v.count++
				block.addReq2WaitList([]*delayReq{v})
			}
		}
	}
}
