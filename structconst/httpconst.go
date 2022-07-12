package structconst

// 返回的结构体

//----------------账号相关---------------

// UserInfo 用户信息
type UserInfo struct {
	Password   string `json:"password"`   // 密码
	CreateTime string `json:"createTime"` // 注册时间
}

// AccountInfo 账号信息
type AccountInfo struct {
	Token    string `json:"token"`    // Token
	Cid      int    `json:"cid"`      // 用户cid
	LastTime string `json:"lastTime"` // 上一次登录时间
	Account  string `json:"account"`  // 账号
}
