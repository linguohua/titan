package node

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/node/scheduler/db/persistent"
	"golang.org/x/xerrors"
)

var myRand = rand.New(rand.NewSource(time.Now().UnixNano()))

// RegisterNode Register a Node
func RegisterNode(nodeType api.NodeType) (api.NodeRegisterInfo, error) {
	info := api.NodeRegisterInfo{}

	deviceID, err := newDeviceID(nodeType)
	if err != nil {
		return info, err
	}

	secret := newSecret(deviceID)

	err = persistent.BindRegisterInfo(secret, deviceID, nodeType)
	if err != nil {
		return info, err
	}

	info.DeviceID = deviceID
	info.Secret = secret

	return info, nil
}

func newDeviceID(nodeType api.NodeType) (string, error) {
	u2 := uuid.New()

	s := strings.Replace(u2.String(), "-", "", -1)
	switch nodeType {
	case api.NodeEdge:
		s = fmt.Sprintf("e_%s", s)
		return s, nil
	case api.NodeCandidate:
		s = fmt.Sprintf("c_%s", s)
		return s, nil
	case api.NodeScheduler:
		s = fmt.Sprintf("s_%s", s)
		return s, nil
	}

	return "", xerrors.Errorf("nodetype err:%d", nodeType)
}

func newSecret(input string) string {
	v := myRand.Intn(100000000)
	input = fmt.Sprintf("%s%d", input, v)

	c := sha1.New()
	c.Write([]byte(input))
	bytes := c.Sum(nil)
	return hex.EncodeToString(bytes)
}
