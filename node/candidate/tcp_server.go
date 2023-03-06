package candidate

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"github.com/linguohua/titan/node/config"
	"net"
	"os"
	"time"

	"github.com/linguohua/titan/api"
)

type tcpMsg struct {
	msgType api.ValidateTcpMsgType
	msg     []byte
	length  int
}

type TCPServer struct {
	Config         *config.CandidateCfg
	BlockWaiterMap *BlockWaiter
}

func NewTCPServer(cfg *config.CandidateCfg, blockWaiterMap *BlockWaiter) *TCPServer {
	return &TCPServer{
		Config:         cfg,
		BlockWaiterMap: blockWaiterMap,
	}
}

func (t *TCPServer) StartTcpServer() {
	tcpAddr, err := net.ResolveTCPAddr("tcp", t.Config.TcpSrvAddr)
	if err != nil {
		log.Fatal(err)
	}

	listen, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		log.Fatal(err)
	}
	// close listener
	defer listen.Close()

	log.Infof("tcp_server listen on %s", t.Config.TcpSrvAddr)

	for {
		conn, err := listen.AcceptTCP()
		if err != nil {
			log.Fatal(err)
			os.Exit(1)
		}

		// conn.SetReadBuffer(104857600)
		go t.handleMessage(conn)
	}
}

func (t *TCPServer) Stop(ctx context.Context) error {
	return nil
}

func (t *TCPServer) handleMessage(conn *net.TCPConn) {
	var now = time.Now()
	var size = int64(0)
	var deviceID = ""

	defer func() {
		if r := recover(); r != nil {
			log.Infof("handleMessage recovered. Error:\n", r)
			return
		}

		conn.Close()
		duration := time.Now().Sub(now)
		bandwidth := float64(size) / float64(duration) * float64(time.Second)
		log.Infof("size:%d, duration:%d, bandwidth:%f, deviceID:%s", size, duration, bandwidth, deviceID)
	}()

	// first item is device id
	tcpMsg, err := readTcpMsg(conn)
	if err != nil {
		log.Errorf("read deviceID error:%v", err)
		return
	}

	if tcpMsg.msgType != api.ValidateTcpMsgTypeDeviceID {
		log.Errorf("read tcp msg error, msg type not ValidateTcpMsgTypeDeviceID")
		return
	}
	deviceID = string(tcpMsg.msg)
	if len(deviceID) == 0 {
		log.Errorf("deviceID is empty")
		return
	}

	bw, exist := t.loadBlockWaiterFromMap(deviceID)
	if !exist {
		log.Errorf("Candidate no wait for device %s", deviceID)
		return
	}

	if bw.conn != nil {
		log.Errorf("device %s aready connect", deviceID)
		return
	}
	bw.conn = conn

	log.Infof("edge node %s connect to candidate, testing bandwidth", deviceID)

	for {
		// next item is file content
		tcpMsg, err = readTcpMsg(conn)
		if err != nil {
			log.Infof("read item error:%v, deviceID:%s", err, deviceID)
			close(bw.ch)
			bw.conn = nil
			return
		}

		size += int64(tcpMsg.length)

		bw.ch <- *tcpMsg
	}
}

func (t *TCPServer) loadBlockWaiterFromMap(key string) (*blockWaiter, bool) {
	vb, exist := t.BlockWaiterMap.Load(key)
	if exist {
		return vb.(*blockWaiter), exist
	}
	return nil, exist
}

func readTcpMsg(conn net.Conn) (*tcpMsg, error) {
	contentLen, err := readContentLen(conn)
	if err != nil {
		return nil, fmt.Errorf("read tcp msgg error %v", err)
	}

	if contentLen <= 0 {
		return nil, fmt.Errorf("pack len %d is invalid", contentLen)
	}

	if contentLen > tcpPackMaxLength {
		return nil, fmt.Errorf("pack len %d is invalid", contentLen)
	}

	buf, err := readBuffer(conn, contentLen)
	if err != nil {
		return nil, fmt.Errorf("read content error %v", err)
	}

	if len(buf) <= 0 {
		return nil, fmt.Errorf("Invalid tcp msg, content len == 0")
	}

	msgType, err := readMsgType(buf[0:1])
	if err != nil {
		return nil, err
	}

	msg := &tcpMsg{msgType: msgType, length: contentLen + 4, msg: buf[1:]}
	// log.Infof("read tcp msg, type:%d, buf len:%d", msg.msgType, len(msg.msg))
	return msg, nil
}

func readMsgType(buf []byte) (api.ValidateTcpMsgType, error) {
	var msgType uint8
	err := binary.Read(bytes.NewReader(buf), binary.LittleEndian, &msgType)
	if err != nil {
		return 0, err
	}

	return api.ValidateTcpMsgType(msgType), nil
}

func readContentLen(conn net.Conn) (int, error) {
	buffer, err := readBuffer(conn, 4)
	if err != nil {
		return 0, err
	}

	var contentLen int32
	err = binary.Read(bytes.NewReader(buffer), binary.LittleEndian, &contentLen)
	if err != nil {
		return 0, err
	}

	return int(contentLen), nil
}

func readBuffer(conn net.Conn, bufferLen int) ([]byte, error) {
	buffer := make([]byte, bufferLen)
	var readLen = 0
	for {
		n, err := conn.Read(buffer[readLen:])
		if err != nil {
			return nil, err
		}

		if n == 0 {
			return nil, fmt.Errorf("buffer len not match, buffer len:%d, current read len:%d", bufferLen, readLen+n)
		}

		readLen += n
		if readLen > len(buffer) {
			return nil, fmt.Errorf("buffer len not match, buffer len:%d, current read len:%d", bufferLen, readLen)
		}

		if len(buffer) == readLen {
			return buffer, nil
		}
	}

}
