package redishelper

import (
	"fmt"

	"github.com/gomodule/redigo/redis"
)

// OtpAccount 验证码账号
type OtpAccount struct {
	Account string
}

// Key key
func (p OtpAccount) Key() string {
	return fmt.Sprintf(RedisKeyCode, p.Account)
}

// Add 按照玩家账号增加验证码 5min过期
func (p OtpAccount) Add(code string) (err error) {
	conn := GetConn()
	defer conn.Close()

	_, err = conn.Do("SETEX", p.Key(), 5*60, code)
	return
}

// Del 删除验证码
func (p OtpAccount) Del() (err error) {
	conn := GetConn()
	defer conn.Close()

	_, err = conn.Do("DEL", p.Key())
	return
}

// Get 获取验证码
func (p OtpAccount) Get() (code string, err error) {
	conn := GetConn()
	defer conn.Close()

	code, err = redis.String(conn.Do("Get", p.Key()))
	return
}
