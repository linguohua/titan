package scheduler

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"

	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/node/scheduler/db/cache"
	"github.com/linguohua/titan/node/scheduler/db/persistent"
	"golang.org/x/xerrors"
)

func registerNode(t api.NodeTypeName) (api.NodeRegisterInfo, error) {
	info := api.NodeRegisterInfo{}

	if t != api.TypeNameEdge && t != api.TypeNameCandidate {
		return info, xerrors.Errorf("type err:%v", t)
	}

	deviceID, err := newDeviceID(t)
	if err != nil {
		return info, err
	}

	secret := newSecret(deviceID)

	err = persistent.GetDB().BindRegisterInfo(secret, deviceID)
	if err != nil {
		return info, err
	}

	info.DeviceID = deviceID
	info.Secret = secret

	return info, nil
}

func newDeviceID(t api.NodeTypeName) (string, error) {
	num, err := cache.GetDB().IncrNodeDeviceID(t)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("%v_%d", t, num), nil
}

func newSecret(input string) string {
	c := sha1.New()
	c.Write([]byte(input))
	bytes := c.Sum(nil)
	return hex.EncodeToString(bytes)
}

func verifySecret(token string) (string, error) {
	deviceID, secret, err := parseToken(token)
	if err != nil {
		return deviceID, xerrors.Errorf("token err:%s,deviceID:%s,secret:%s", err.Error(), deviceID, secret)
	}

	s, err := persistent.GetDB().GetSecretInfo(deviceID)
	if err != nil {
		return deviceID, xerrors.Errorf("info err:%s,deviceID:%s,secret:%s", err.Error(), deviceID, secret)
	}

	if s != secret {
		return deviceID, xerrors.Errorf("err:%s,deviceID:%s,secret:%s,s:%s", "secret mismatch", deviceID, secret, s)
	}

	return deviceID, nil
}
