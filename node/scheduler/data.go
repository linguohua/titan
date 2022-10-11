package scheduler

// Data Data
type Data struct {
	id     string
	cid    string
	caches []*Cache
	// 可靠性 PS caches可靠性决定
	reliability int
}

func newData(cid string, reliability int, area string) {
	// 根据可靠性 reliability 决定需要 cache 几份 (分别 cache 在哪些 node 上)
	// 1.检查 cid 是否已经有 data 数据
	// 1.1 有data数据 则看可靠性是否达到要求, 没达到要求则 增强可靠性 (多cache几份)
	// 1.2 没data数据 则创建新的data数据
	// 2.根据策略(待确定) 选择某个节点cache数据,并且返回数据里信息
	// 2.1 如果此数据还有子数据,则继续请求节点走第2步
	// 2.2 如果已经没有子数据,则计算可靠性,并记录data信息
	// 3.根据策略(待确定) 把第2步中的data分多份cache,cache到多个边缘节点上
	// 3.1 等待边缘节点返回结果,计算可靠性,直到到达要求
}
