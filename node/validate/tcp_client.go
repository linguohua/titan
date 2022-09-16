package validate

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"

	"github.com/linguohua/titan/node/download"
	"golang.org/x/time/rate"
)

func newTcpClient(addr string) (*net.TCPConn, error) {
	tcpAddr, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		return nil, err
	}

	return net.DialTCP("tcp", nil, tcpAddr)
}

func packData(data []byte) ([]byte, error) {
	lenBuf := new(bytes.Buffer)
	contentLen := int32(len(data))
	err := binary.Write(lenBuf, binary.LittleEndian, contentLen)
	if err != nil {
		return nil, err
	}

	buf := make([]byte, len(data)+4)
	copy(buf[0:4], lenBuf.Bytes())

	if len(data) > 0 {
		copy(buf[4:], data)
	}

	return buf, nil
}

func sendData(conn *net.TCPConn, data []byte, limiter *rate.Limiter) error {
	buf, err := packData(data)
	if err != nil {
		return err
	}

	n, err := io.Copy(conn, download.NewReader(bytes.NewBuffer(buf), limiter))
	if err != nil {
		log.Errorf("sendData, io.Copy error:%v", err)
		return err
	}

	if int(n) != len(buf) {
		return fmt.Errorf("Send data len is %d, but buf len is %d", n, len(buf))
	}

	return nil
}

func sendDeviceID(conn *net.TCPConn, deviceID string, limiter *rate.Limiter) error {
	if len(deviceID) == 0 {
		return fmt.Errorf("deviceID can not empty")
	}

	return sendData(conn, []byte(deviceID), limiter)
}
