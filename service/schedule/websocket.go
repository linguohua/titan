package schedule

import (
	"net/http"

	"titan-ultra-network/log"

	"github.com/gorilla/websocket"
	"github.com/julienschmidt/httprouter"
)

const (
	wsReadLimit       = 2048 // 2k
	wsReadBufferSize  = 2048 // 2k
	wsWriteBufferSize = 4096 // 4k
)

var upgrader = websocket.Upgrader{ReadBufferSize: wsReadBufferSize, WriteBufferSize: wsWriteBufferSize}

// AcceptWebsocket 长连接
func AcceptWebsocket(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	// cl := log.WithField("peer", retrieveClientAddr(r))

	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		// cl.Println(err)
		log.Errorf("AcceptWebsocket Upgrade err :%v", err)
		return
	}

	// 激活码
	deviceID := params.ByName("deviceid")
	// q := r.URL.Query()

	// ensure websocket connection will be closed anyway
	defer ws.Close()
	// read limit
	ws.SetReadLimit(wsReadLimit)

	log.Infoln("accept websocket:", r.URL)

	userID, deviceType := getTypeAndUserID(deviceID)
	acceptDevice(userID, deviceID, deviceType, ws, r)
}

func getTypeAndUserID(deviceID string) (string, DeviceType) {
	// TODO 根据激活码 获取节点类型(边缘节点,候选节点)、用户ID
	return "", DeviceTypeEdge
}

func acceptDevice(userID, deviceID string, deviceType DeviceType, ws *websocket.Conn, r *http.Request) {
	// found target table
	device := newDevice(ws, userID, deviceID, deviceType)

	if device != nil {

		devices = append(devices, device)

		drainDeviceWebsocket(device, ws)
	}
}

func drainDeviceWebsocket(device *Device, ws *websocket.Conn) {
	ws.SetPongHandler(func(msg string) error {
		device.OnPong(ws, msg)
		return nil
	})

	ws.SetPingHandler(func(msg string) error {
		device.OnPing(ws, msg)
		return nil
	})

	// var errExit error
	// ensure to notify player that we exit websocket reading anyway
	// defer device.OnExitMsgLoop(ws, errExit)

	for {
		mt, message, err := ws.ReadMessage()
		if err != nil {
			// errExit = err
			ws.Close()
			break
		}

		if message != nil && len(message) > 0 && mt == websocket.BinaryMessage {
			device.OnWebsocketMessage(ws, message)
		}
	}
}
