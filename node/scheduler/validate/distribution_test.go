package validate

import (
	"fmt"
	"github.com/linguohua/titan/api"
	"testing"
)

func TestGetAvgArr(t *testing.T) {
	list := []validatedDeviceInfo{
		{deviceId: "node01", nodeType: api.NodeCandidate, addr: "addr01", bandwidth: 500.21},
		{deviceId: "node02", nodeType: api.NodeEdge, addr: "addr02", bandwidth: 18.0},
		{deviceId: "node03", nodeType: api.NodeEdge, addr: "addr03", bandwidth: 28.0},
		{deviceId: "node04", nodeType: api.NodeEdge, addr: "addr04", bandwidth: 2.0},
		{deviceId: "node05", nodeType: api.NodeEdge, addr: "addr05", bandwidth: 27.0},
		{deviceId: "node06", nodeType: api.NodeEdge, addr: "addr06", bandwidth: 35.0},
		{deviceId: "node07", nodeType: api.NodeEdge, addr: "addr07", bandwidth: 22.0},
		{deviceId: "node08", nodeType: api.NodeEdge, addr: "addr08", bandwidth: 10.0},
		{deviceId: "node09", nodeType: api.NodeEdge, addr: "addr09", bandwidth: 6.0},
		{deviceId: "node10", nodeType: api.NodeEdge, addr: "addr10", bandwidth: 5.0},
		{deviceId: "node11", nodeType: api.NodeEdge, addr: "addr11", bandwidth: 3.0},
		{deviceId: "node12", nodeType: api.NodeEdge, addr: "addr12", bandwidth: 2.0},
		{deviceId: "node13", nodeType: api.NodeEdge, addr: "addr13", bandwidth: 1.0},
	}
	fmt.Println("test list is : ", list)
	fmt.Println("result is :")
	arrays := GetAverageArray(list, 10)
	for i, arr := range arrays {
		fmt.Printf("arr %d is : %+v, ", i, arr)
		var sumValue float64
		for _, value := range arr {
			sumValue += value.bandwidth
		}
		fmt.Println("sum = ", sumValue)
	}
}
