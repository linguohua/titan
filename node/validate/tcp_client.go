package validate

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"

	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/lib/limiter"
	"golang.org/x/time/rate"
)

func newTcpClient(addr string) (*net.TCPConn, error) {
	tcpAddr, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		return nil, err
	}

	return net.DialTCP("tcp", nil, tcpAddr)
}

func packData(data []byte, msgType api.ValidateTcpMsgType) ([]byte, error) {
	// msg type is uint8
	msgTypeLen := 1
	contentLen := int32(msgTypeLen + len(data))

	lenBuf := new(bytes.Buffer)
	err := binary.Write(lenBuf, binary.LittleEndian, contentLen)
	if err != nil {
		return nil, err
	}

	msgTypeBuf := new(bytes.Buffer)
	err = binary.Write(msgTypeBuf, binary.LittleEndian, uint8(msgType))
	if err != nil {
		return nil, err
	}

	buf := make([]byte, contentLen+4)
	copy(buf[0:4], lenBuf.Bytes())
	copy(buf[4:5], msgTypeBuf.Bytes())

	if len(data) > 0 {
		copy(buf[5:], data)
	}

	return buf, nil
}

func sendData(conn *net.TCPConn, data []byte, msgType api.ValidateTcpMsgType, rateLimiter *rate.Limiter) error {
	buf, err := packData(data, msgType)
	if err != nil {
		return err
	}

	n, err := io.Copy(conn, limiter.NewReader(bytes.NewBuffer(buf), rateLimiter))
	if err != nil {
		log.Errorf("sendData, io.Copy error:%s", err.Error())
		return err
	}

	if int(n) != len(buf) {
		return fmt.Errorf("Send data len is %d, but buf len is %d", n, len(buf))
	}

	return nil
}

func sendNodeID(conn *net.TCPConn, nodeID string, limiter *rate.Limiter) error {
	if len(nodeID) == 0 {
		return fmt.Errorf("nodeID can not empty")
	}

	return sendData(conn, []byte(nodeID), api.ValidateTcpMsgTypeNodeID, limiter)
}
