package scheduler

import (
	"crypto/sha1"
	"encoding/hex"
	"strings"

	"github.com/google/uuid"
	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/node/scheduler/db/persistent"
	"golang.org/x/xerrors"
)

func registerNode() (api.NodeRegisterInfo, error) {
	info := api.NodeRegisterInfo{}

	deviceID, err := newDeviceID()
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

func newDeviceID() (string, error) {
	u2, err := uuid.NewUUID()
	if err != nil {
		return "", err
	}

	s := strings.Replace(u2.String(), "-", "", -1)

	return s, nil
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
