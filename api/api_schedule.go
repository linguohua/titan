package api

// 调度中心api

// Schedule 调度中心函数
type Schedule interface {
	GetTime() (string, error)
}
