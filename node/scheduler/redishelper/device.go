package redishelper

import "fmt"

const (
	// redis field
	lastTimeField = "LastTimeField"
)

// SaveDeciceInfo Save DeciceInfo
func SaveDeciceInfo(deviceID string, lastTime string) error {
	conn := getConn()
	defer conn.Close()

	key := fmt.Sprintf(RedisKeyDeviceInfo, deviceID)
	_, err := conn.Do("HSET", key, lastTimeField, lastTime)
	if err != nil {
		return err
	}

	return nil
}

// LoadDeciceInfo Load DeciceInfo
func LoadDeciceInfo(deviceID string) error {
	return nil
}
