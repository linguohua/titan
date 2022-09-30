package block

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"
	"github.com/linguohua/titan/api/client"
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
	client := &http.Client{}
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

func getCandidate(candidateURL string) (*Candidate, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	api, close, err := client.NewCandicate(ctx, candidateURL, nil)
	if err != nil {
		log.Errorf("getCandidateAPI, NewCandicate err:%v", err)
		return nil, err
	}
	defer close()

	info, err := api.DeviceInfo(ctx)
	if err != nil {
		log.Errorf("getCandidateAPI, NewCandicate err:%v", err)
		return nil, err
	}

	download, err := api.GetDownloadInfo(ctx)
	if err != nil {
		log.Errorf("getCandidateAPI, NewCandicate err:%v", err)
		return nil, err
	}

	candidate := &Candidate{deviceID: info.DeviceId, downSrvURL: download.URL, token: download.Token}
	return candidate, nil
}

func getCandidateWithMap(candidateMap map[string]*Candidate, candidateURL string) (*Candidate, error) {
	var err error
	candidate, ok := candidateMap[candidateURL]
	if !ok {
		candidate, err = getCandidate(candidateURL)
		if err != nil {
			return nil, err
		}
		candidateMap[candidateURL] = candidate
	}
	return candidate, nil
}

func loadBlocksFromCandidate(block *Block, reqs []*delayReq) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	candidateMap := make(map[string]*Candidate)
	for _, req := range reqs {
		candidate, err := getCandidateWithMap(candidateMap, req.candidateURL)
		if err != nil {
			log.Errorf("getCandidateWithMap error:%v", err)
			block.cacheResult(ctx, req.cid, "", err)
			continue
		}

		url := fmt.Sprintf("%s?cid=%s", candidate.downSrvURL, req.cid)

		data, err := getBlockFromCandidate(url, candidate.token)
		if err == nil {
			err = block.blockStore.Put(req.cid, data)
		}

		block.cacheResult(ctx, req.cid, candidate.deviceID, err)
		log.Infof("loadBlocksFromCandidate, cid:%s,err:%v", req.cid, err)

		links, err := getLinks(block, data, req.cid)
		if err != nil {
			log.Errorf("loadBlocksFromIPFS resolveLinks error:%s", err.Error())
			continue
		}

		if len(links) > 0 {
			delayReqs := make([]*delayReq, 0, len(links))
			for _, link := range links {
				dReq := &delayReq{}
				dReq.cid = link.Cid.String()
				dReq.candidateURL = req.candidateURL
				delayReqs = append(delayReqs, dReq)
			}

			block.addReq2WaitList(delayReqs)
		}
	}
}

func getLinks(block *Block, data []byte, cidStr string) ([]*format.Link, error) {
	if len(data) == 0 {
		return make([]*format.Link, 0), nil
	}

	target, err := cid.Decode(cidStr)
	if err != nil {
		return make([]*format.Link, 0), err
	}

	blk, err := blocks.NewBlockWithCid(data, target)
	if err != nil {
		return make([]*format.Link, 0), err
	}

	return block.resolveLinks(blk)
}
