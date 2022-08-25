package candidate

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"time"
)

func ListenTCP(address string) {
	fmt.Printf("Listen on %s", address)
	tcpAddr, err := net.ResolveTCPAddr("tcp", address)
	if err != nil {
		panic(err)
	}

	listen, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		log.Fatal(err)
		os.Exit(1)
	}

	// close listener
	defer listen.Close()
	for {
		conn, err := listen.AcceptTCP()
		if err != nil {
			log.Fatal(err)
			os.Exit(1)
		}

		// conn.SetReadBuffer(104857600)
		// conn.SetWriteBuffer(104857600)
		go handleMessage(conn)
	}
}

func handleMessage(conn net.Conn) {
	defer conn.Close()
	var now = time.Now()
	var size = int64(0)

	defer func() {
		duration := time.Now().Sub(now)
		bandwidth := float64(size) / float64(duration) * 1000000000
		log.Infof("size:%d, duration:%d, bandwidth:%f", size, duration, bandwidth)
	}()

	for {
		contentLen, err := readContentLen(conn)
		if err != nil {
			log.Infof("read content len error:%v", err)
			return
		}

		// log.Infof("recev len:%d", contentLen)

		_, err = readContent(conn, int(contentLen))
		if err != nil {
			log.Infof("read content error:%v", err)
			return
		}

		size += int64(contentLen) + 4
		// cid, err := cidFromData(content)
		// if err != nil {
		// 	log.Infof("cidFromData error:%v", err)
		// 	return
		// }

		// log.Infof("cid :%s", cid)
	}
}

func readContentLen(conn net.Conn) (int, error) {
	buffer := make([]byte, 4)
	_, err := conn.Read(buffer)
	if err != nil {
		log.Infof("read len error:%v", err)
		return 0, err
	}

	var contentLen int32
	err = binary.Read(bytes.NewReader(buffer), binary.LittleEndian, &contentLen)
	if err != nil {
		fmt.Println("binary.Read failed:", err)
		return 0, err
	}

	return int(contentLen), nil
}

func readContent(conn net.Conn, conotentLen int) ([]byte, error) {
	content := make([]byte, 0, conotentLen)
	var readLen = 128
	for {
		if conotentLen-len(content) < 128 {
			readLen = conotentLen - len(content)
		}

		buffer := make([]byte, readLen)
		n, err := conn.Read(buffer)
		if err != nil {
			return nil, err
		}

		if n == 0 {
			return nil, fmt.Errorf("Content len not match, content len:%d, current read len:%d", conotentLen, len(content)+n)
		}

		if len(content)+n > conotentLen {
			return nil, fmt.Errorf("Content len not match, content len:%d, current read len:%d", conotentLen, len(content)+n)
		}

		if n > 0 {
			content = append(content, buffer[0:n]...)
		}

		if len(content) == conotentLen {
			return content, nil
		}
	}

}
