package api

type OpenRPCDocument map[string]interface{}

type IndexRequest struct {
	UserId string
}

// 首页加载数据
type IndexPageRes struct {
	//AllMinerNum MinerInfo
	AllMinerInfo
	//OnlineMinerNum MinerInfo
	OnlineVerifier  int `json:"online_verifier"`  // 在线验证人
	OnlineCandidate int `json:"online_candidate"` // 在线候选人
	OnlineEdgeNode  int `json:"online_edge_node"` // 在线边缘节点
	//ProfitInfo Profit  // 个人收益信息
	CumulativeProfit float64 `json:"cumulative_profit"` // 个人累计收益
	YesterdayProfit  float64 `json:"yesterday_profit"`  // 昨日收益
	TodayProfit      float64 `json:"today_profit"`      // 今日收益
	SevenDaysProfit  float64 `json:"seven_days_profit"` // 近七天收益
	MonthProfit      float64 `json:"month_profit"`      // 近30天收益
	//Device Devices // 设备信息
	TotalNum    int `json:"total_num"`    // 设备总数
	OnlineNum   int `json:"online_num"`   // 在线设备数
	OfflineNum  int `json:"offline_num"`  // 离线设备数
	AbnormalNum int `json:"abnormal_num"` // 异常设备数
}

type AllMinerInfo struct {
	AllVerifier  int     `json:"all_verifier"`  // 全网验证人
	AllCandidate int     `json:"all_candidate"` // 全网候选人
	AllEdgeNode  int     `json:"all_edgeNode"`  // 全网边缘节点
	StorageT     float64 `json:"storage_t"`     // 全网存储（T）
	BandwidthMb  float64 `json:"bandwidth_mb"`  // 全网上行带宽（MB/S）
}
