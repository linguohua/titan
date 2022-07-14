package edge

import (
	"titan-ultra-network/cache"
)

// 边缘节点逻辑

// Init 初始化
func Init() {
	// 请求调度中心,加入网络
}

// GetData 接到上级指示 获取数据
func GetData() {
	cache.Load()
}

// SaveData 接到上级指示 缓存数据
func SaveData() {
	cache.Save()
}
