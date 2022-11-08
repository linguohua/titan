package scheduler

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/node/scheduler/db/persistent"
	"golang.org/x/xerrors"
)

func registerNode(nodeType api.NodeType) (api.NodeRegisterInfo, error) {
	info := api.NodeRegisterInfo{}

	deviceID, err := newDeviceID(nodeType)
	if err != nil {
		return info, err
	}

	secret := newSecret(deviceID)

	err = persistent.GetDB().BindRegisterInfo(secret, deviceID, nodeType)
	if err != nil {
		return info, err
	}

	info.DeviceID = deviceID
	info.Secret = secret

	return info, nil
}

func newDeviceID(nodeType api.NodeType) (string, error) {
	u2, err := uuid.NewUUID()
	if err != nil {
		return "", err
	}

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
	c := sha1.New()
	c.Write([]byte(input))
	bytes := c.Sum(nil)
	return hex.EncodeToString(bytes)
}

func verifySecret(token string, nodeType api.NodeType) (string, error) {
	deviceID, secret, err := parseToken(token)
	if err != nil {
		return deviceID, xerrors.Errorf("token err:%s,deviceID:%s,secret:%s", err.Error(), deviceID, secret)
	}

	return deviceID, nil

	// info, err := persistent.GetDB().GetRegisterInfo(deviceID)
	// if err != nil {
	// 	return deviceID, xerrors.Errorf("info err:%s,deviceID:%s,secret:%s", err.Error(), deviceID, secret)
	// }

	// if info.Secret != secret {
	// 	return deviceID, xerrors.Errorf("err:%s,deviceID:%s,secret:%s,info_s:%s", "secret mismatch", deviceID, secret, info.Secret)
	// }

	// if info.NodeType != int(nodeType) {
	// 	return deviceID, xerrors.Errorf("err:%s,deviceID:%s,nodeType:%v,info_n:%v", "node type mismatch", deviceID, nodeType, info.NodeType)
	// }

	// return deviceID, nil
}
