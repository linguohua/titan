package candidate

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"sync"
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
	schedulerAPI   api.Scheduler
	config         *config.CandidateCfg
	blockWaiterMap *sync.Map
}

func NewTCPServer(cfg *config.CandidateCfg, schedulerAPI api.Scheduler) *TCPServer {
	return &TCPServer{
		config:         cfg,
		blockWaiterMap: &sync.Map{},
		schedulerAPI:   schedulerAPI,
	}
}

func (t *TCPServer) StartTCPServer() {
	tcpAddr, err := net.ResolveTCPAddr("tcp", t.config.TCPSrvAddr)
	if err != nil {
		log.Fatal(err)
	}

	listen, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		log.Fatal(err)
	}
	// close listener
	defer listen.Close() //nolint:errcheck // ignore error

	log.Infof("tcp_server listen on %s", t.config.TCPSrvAddr)

	for {
		conn, err := listen.AcceptTCP()
		if err != nil {
			log.Fatal(err)
			os.Exit(1)
		}

		go t.handleMessage(conn)
	}
}

func (t *TCPServer) Stop(ctx context.Context) error {
	return nil
}

func (t *TCPServer) handleMessage(conn *net.TCPConn) {
	defer func() {
		if r := recover(); r != nil {
			log.Infof("handleMessage recovered. Error:\n", r)
			return
		}
	}()

	// first item is device id
	msg, err := readTCPMsg(conn)
	if err != nil {
		log.Errorf("read nodeID error:%v", err)
		return
	}

	if msg.msgType != api.TCPMsgTypeNodeID {
		log.Errorf("read tcp msg error, msg type not ValidateTCPMsgTypeNodeID")
		return
	}
	nodeID := string(msg.msg)
	if len(nodeID) == 0 {
		log.Errorf("nodeID is empty")
		return
	}

	_, ok := t.blockWaiterMap.Load(nodeID)
	if ok {
		log.Errorf("Validator already wait for device %s", nodeID)
		return
	}

	ch := make(chan tcpMsg, 1)
	defer close(ch)

	bw := newBlockWaiter(nodeID, ch, t.config.ValidateDuration, t.schedulerAPI)
	t.blockWaiterMap.Store(nodeID, bw)

	log.Debugf("edge node %s connect to Validator", nodeID)

	timer := time.NewTimer(time.Duration(t.config.ValidateDuration) * time.Second)
	for {
		select {
		case <-timer.C:
			conn.Close()
			return
		default:
		}
		// next item is file content
		msg, err = readTCPMsg(conn)
		if err != nil {
			log.Infof("read item error:%v, nodeID:%s", err, nodeID)
			return
		}
		bw.ch <- *msg

		if msg.msgType == api.TCPMsgTypeCancel {
			conn.Close()
			return
		}
	}
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
