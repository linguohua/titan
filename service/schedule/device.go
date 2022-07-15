package schedule

import (
	"sync"
	"time"

	"titan-ultra-network/log"

	"github.com/gorilla/websocket"
)

const (
	websocketWriteDeadLine = 3 * time.Second
)

// DeviceType 设备类型
type DeviceType int

const (
	// DeviceTypeEdge 边缘节点
	DeviceTypeEdge DeviceType = 3
	// DeviceTypeCandidate 候选节点
	DeviceTypeCandidate DeviceType = 2
	// DeviceTypeValidator 验证节点
	DeviceTypeValidator DeviceType = 1
	// DeviceTypeSchedule 调度中心
	DeviceTypeSchedule DeviceType = 0
)

// Device 设备
type Device struct {
	userID string

	deviceID string // 激活码

	lastMsgTime time.Time

	wsWriteLock sync.Mutex

	// websocket connection
	ws *websocket.Conn

	dType DeviceType
}

func newDevice(ws *websocket.Conn, userID, deviceNum string, deviceType DeviceType) *Device {
	p := &Device{
		userID:   userID,
		deviceID: deviceNum,
		ws:       ws,
		dType:    deviceType,
	}

	return p
}

// OnPong handle pong message
func (p *Device) OnPong(ws *websocket.Conn, msg string) {
	if p.ws != ws {
		return
	}

	p.lastMsgTime = time.Now()

	// TODO: calc RTT and store
}

// OnPing handle ping message
func (p *Device) OnPing(ws *websocket.Conn, msg string) {
	if p.ws != ws {
		return
	}

	p.send([]byte(msg))

	p.lastMsgTime = time.Now()
}

// send write buffer to player's websocket connection
func (p *Device) send(bytes []byte) {
	ws := p.ws
	if ws != nil {
		p.wsWriteLock.Lock()
		defer p.wsWriteLock.Unlock()

		// write deadline for write timeout
		ws.SetWriteDeadline(time.Now().Add(websocketWriteDeadLine))
		err := ws.WriteMessage(websocket.BinaryMessage, bytes)
		if err != nil {
			ws.Close()
			log.Errorf("web socket write err:%v", err)
			// p.cl.Println("web socket write err:", err)
		}
	}
}

// OnWebsocketMessage handle player's network message
func (p *Device) OnWebsocketMessage(ws *websocket.Conn, msg []byte) {
	if p.ws != ws {
		return
	}

	p.lastMsgTime = time.Now()
}

func (p *Device) sendGameMsg(bytes []byte) {
	p.send(bytes)
}
