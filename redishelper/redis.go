package redishelper

import (
	"sync"
	"time"

	"github.com/gomodule/redigo/redis"
)

var (
	pool *redis.Pool

	once sync.Once

	addr string
)

const (
	//---------------账号相关---------------

	// RedisKeyUserInfo 用户信息 cid (保存不经常变化的重要信息)
	RedisKeyUserInfo = "FilecoinBrowserHelper:UserInfo:%d"
	// RedisKeyAccountInfo 账号信息 account (保存经常变化的不重要信息)
	RedisKeyAccountInfo = "FilecoinBrowserHelper:AccountInfo:%s"
	// RedisKeyUserCid Cid
	RedisKeyUserCid = "FilecoinBrowserHelper:UserCid"
	// RedisKeyCode 验证码 email
	RedisKeyCode = "FilecoinBrowserHelper:Code:%s"
)

// newPool 新建redis连接池
func newPool(addr string) *redis.Pool {
	return &redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,
		Dial:        func() (redis.Conn, error) { return redis.Dial("tcp", addr) },
	}
}

// Init 初始化
func Init(address string) {
	addr = address
}

// GetConn get a redis connection
func GetConn() redis.Conn {
	if pool == nil {
		once.Do(func() {
			pool = newPool(addr)
		})
	}

	return pool.Get()
}
