package node

import (
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/node/scheduler/db/persistent"
	"golang.org/x/xerrors"
)

// Allocate allocate a Node
func Allocate(nodeType api.NodeType) (api.NodeAllocateInfo, error) {
	info := api.NodeAllocateInfo{}

	deviceID, err := newDeviceID(nodeType)
	if err != nil {
		return info, err
	}

	secret := newSecret()

	err = persistent.BindNodeAllocateInfo(secret, deviceID, nodeType)
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
	}

	return "", xerrors.Errorf("nodetype err:%d", nodeType)
}

func newSecret() string {
	uStr := uuid.NewString()

	return strings.Replace(uStr, "-", "", -1)
}
