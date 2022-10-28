package scheduler

var serverArea = "CN-GD-Shenzhen"

// InitServerArea set area
func InitServerArea(area string) {
	log.Infof("server area :%s", area)
	serverArea = area
}

func initAreaTable() {
}

func areaExist(area string) bool {
	if area == "" {
		return false
	}

	return area == serverArea
}
