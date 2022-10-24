package scheduler

var areaPool = []string{"CN-GD-Shenzhen"}

func initAreaTable() {
}

func areaExist(area string) bool {
	if area == "" {
		return false
	}

	for _, a := range areaPool {
		if a == area {
			return true
		}
	}

	return false
}
