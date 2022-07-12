package httpclient

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
)

// Request 请求
func Request(url string, bodyMarshal []byte, isPost bool) ([]byte, error) {
	// 地址服务的 地址端口
	// url := fmt.Sprintf("http://%s/%s", addressServer, path)
	var resp *http.Response
	var err error
	if isPost {
		resp, err = httpPost(url, bodyMarshal)
	} else {
		resp, err = httpGet(url)
	}
	if err != nil {
		// fmt.Println("err:", err.Error())
		return nil, err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Println(err.Error())
		return nil, err
	}

	// fmt.Println(string(body))
	return body, nil
}

func httpPost(url string, bodyMarshal []byte) (*http.Response, error) {
	resp, err := http.Post(url,
		"application/json",
		strings.NewReader(string(bodyMarshal)),
	)
	return resp, err
}

func httpGet(url string) (*http.Response, error) {
	resp, err := http.Get(url)
	return resp, err
}
