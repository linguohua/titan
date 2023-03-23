package candidate

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"time"

	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/node/config"
)

type tcpMsg struct {
	msgType api.TCPMsgType
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

func (t *TCPServer) StartTCPServer() {
	tcpAddr, err := net.ResolveTCPAddr("tcp", t.Config.TCPSrvAddr)
	if err != nil {
		log.Fatal(err)
	}

	listen, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		log.Fatal(err)
	}
	// close listener
	defer listen.Close()

	log.Infof("tcp_server listen on %s", t.Config.TCPSrvAddr)

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
	now := time.Now()
	size := int64(0)
	nodeID := ""

	defer func() {
		if r := recover(); r != nil {
			log.Infof("handleMessage recovered. Error:\n", r)
			return
		}

		conn.Close()
		duration := time.Since(now)
		bandwidth := float64(size) / float64(duration) * float64(time.Second)
		log.Infof("size:%d, duration:%d, bandwidth:%f, nodeID:%s", size, duration, bandwidth, nodeID)
	}()

	// first item is device id
	tcpMsg, err := readTCPMsg(conn)
	if err != nil {
		log.Errorf("read nodeID error:%v", err)
		return
	}

	if tcpMsg.msgType != api.TCPMsgTypeNodeID {
		log.Errorf("read tcp msg error, msg type not ValidateTCPMsgTypeNodeID")
		return
	}
	nodeID = string(tcpMsg.msg)
	if len(nodeID) == 0 {
		log.Errorf("nodeID is empty")
		return
	}

	bw, exist := t.loadBlockWaiterFromMap(nodeID)
	if !exist {
		log.Errorf("Candidate no wait for device %s", nodeID)
		return
	}

	if bw.conn != nil {
		log.Errorf("device %s already connect", nodeID)
		return
	}
	bw.conn = conn

	log.Infof("edge node %s connect to candidate, testing bandwidth", nodeID)

	for {
		// next item is file content
		tcpMsg, err = readTCPMsg(conn)
		if err != nil {
			log.Infof("read item error:%v, nodeID:%s", err, nodeID)
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

func readTCPMsg(conn net.Conn) (*tcpMsg, error) {
	contentLen, err := readContentLen(conn)
	if err != nil {
		return nil, fmt.Errorf("read tcp msg error %v", err)
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
		return nil, fmt.Errorf("invalid tcp msg, content len == 0")
	}

	msgType, err := readMsgType(buf[0:1])
	if err != nil {
		return nil, err
	}

	msg := &tcpMsg{msgType: msgType, length: contentLen + 4, msg: buf[1:]}
	// log.Infof("read tcp msg, type:%d, buf len:%d", msg.msgType, len(msg.msg))
	return msg, nil
}

func readMsgType(buf []byte) (api.TCPMsgType, error) {
	var msgType uint8
	err := binary.Read(bytes.NewReader(buf), binary.LittleEndian, &msgType)
	if err != nil {
		return 0, err
	}

	return api.TCPMsgType(msgType), nil
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
	readLen := 0
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
