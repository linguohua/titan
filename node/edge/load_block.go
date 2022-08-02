package edge

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"
)

// just test
func loadBlock(cid string) ([]byte, error) {
	url := "https://ipfs.io/api/v0/block/get?arg=%s"
	url = fmt.Sprintf(url, cid)

	c := http.Client{Timeout: time.Duration(10) * time.Second}
	resp, err := c.Post(url, "", nil)
	if err != nil {
		fmt.Printf("Error %s", err)
		return nil, err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	// fmt.Printf("Body : %s", body)
	return body, nil
}

func loadBlocks(edge EdgeAPI, cids []string) {
	ct := context.Background()

	for _, cid := range cids {
		// TODO: need to check block if exist in local store
		data, err := loadBlock(cid)
		if err != nil {
			log.Errorf("CacheData, loadBlock error:", err)
			err = edge.scheduler.CacheResult(ct, edge.deviceID, cid, false)
			if err != nil {
				log.Errorf("CacheData, CacheResult error:", err)
			}
			continue
		}

		err = edge.blockStore.Put(data, cid)
		if err != nil {
			log.Errorf("CacheData, put error:", err)
			err = edge.scheduler.CacheResult(ct, edge.deviceID, cid, false)
			if err != nil {
				log.Errorf("CacheData, CacheResult error:", err)
			}
			continue
		}

		err = edge.scheduler.CacheResult(ct, edge.deviceID, cid, true)
		if err != nil {
			log.Errorf("CacheData, CacheResult error:", err)
		}

		log.Infof("cache data,cid:%s", cid)
	}
}
