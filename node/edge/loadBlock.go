package edge

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"time"
)

// 测试用
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
