package edge

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"time"
)

// 测试用
func loadBlock(cid string) ([]byte, error) {
	url := "http://127.0.0.1:5001/api/v0/block/get?arg=%s"
	url = fmt.Sprintf(url, cid)

	c := http.Client{Timeout: time.Duration(1) * time.Second}
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
