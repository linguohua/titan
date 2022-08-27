package edge

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
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

func sendData(conn *net.TCPConn, data []byte) error {
	buf, err := packData(data)
	if err != nil {
		return err
	}

	n, err := conn.Write(buf)
	if err != nil {
		return err
	}

	if n != len(buf) {
		return fmt.Errorf("Send len is %d, but buf len is %d", n, len(buf))
	}

	return nil
}

func sendDeviceID(conn *net.TCPConn, deviceID string) error {
	if len(deviceID) == 0 {
		return fmt.Errorf("len(deviceID) == 0")
	}

	return sendData(conn, []byte(deviceID))
}
