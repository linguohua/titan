package errorcode

// ErrorCode 错误码
type ErrorCode int

const (
	// Success 正确
	Success ErrorCode = 0
	// Unmarshal 解析错误
	Unmarshal ErrorCode = 1
	// Marshal 编码错误
	Marshal ErrorCode = 2
	// DataIsNull 空数据
	DataIsNull ErrorCode = 3
	// ReadBody 读取body出错
	ReadBody ErrorCode = 4
	// Parameters 参数错误
	Parameters ErrorCode = 5
	// EmailFormat 邮箱格式
	EmailFormat ErrorCode = 6
	// RedisError 操作Redis失败
	RedisError ErrorCode = 7
	// AccountExists 账号已存在
	AccountExists ErrorCode = 8
	// AccountNotExists 账号不存在
	AccountNotExists ErrorCode = 9
	// PasswordMismatch 密码不正确
	PasswordMismatch ErrorCode = 10
	// GenerateToken 生成Token失败
	GenerateToken ErrorCode = 11
	// CodeMismatch 验证码不正确
	CodeMismatch ErrorCode = 12
	// UserNotExists 用户不存在
	UserNotExists ErrorCode = 13
	// ExceedLimit 超出限制
	ExceedLimit ErrorCode = 14
	// NameExists 名称已存在
	NameExists ErrorCode = 15
	// PhoneFormat 手机格式
	PhoneFormat ErrorCode = 16
)
