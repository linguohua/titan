package validation

import (
	"fmt"
	"testing"
	"time"

	"github.com/alecthomas/units"
)

var bandwidthDown = []float64{
	float64(100 * units.MiB),
	float64(200 * units.MiB),
	float64(500 * units.MiB),
	float64(1000 * units.MiB),
	float64(2000 * units.MiB),
}

var bandwidthUp = []float64{
	float64(5 * units.MiB),
	float64(10 * units.MiB),
	float64(20 * units.MiB),
	float64(50 * units.MiB),
	float64(100 * units.MiB),
	float64(200 * units.MiB),
}

func Test1(t *testing.T) {
	nodeManager := &Manager{
		unpairedGroup: newValidatableGroup(),
	}

	s := 10000
	vi := initValidator(nodeManager, "c_1", 0, s)
	vi += initValidator(nodeManager, "c_2", 1, s)
	vi += initValidator(nodeManager, "c_3", 2, s)
	vi += initValidator(nodeManager, "c_4", 3, s)
	vi += initValidator(nodeManager, "c_5", 4, s)

	num := vi / float64(100*units.MiB)
	fmt.Printf("vi:%.2f , num:%.2f \n", vi, num)

	s = 50000
	initBeValidate(nodeManager, "e_1", 0, s)
	initBeValidate(nodeManager, "e_2", 1, s)
	initBeValidate(nodeManager, "e_3", 2, s)
	initBeValidate(nodeManager, "e_4", 3, s)
	initBeValidate(nodeManager, "e_5", 4, s)
	initBeValidate(nodeManager, "e_6", 5, s)

	fmt.Printf("start init selectValidators %s \n", time.Now().String())
	nodeManager.PairValidatorsAndValidatableNodes()

	fmt.Printf("start end %s \n", time.Now().String())

	for i, b := range nodeManager.vWindows {
		fs := 0
		s := ""
		for id, f := range b.ValidatableNodes {
			fs += int(f)
			s = fmt.Sprintf("%s,%s", s, id)
		}
		// fmt.Printf("node:%s,fs:%d %s\n", b.NodeID, fs, s)

		if i > 50 {
			return
		}
	}
}

func initValidator(nodeManager *Manager, name string, index, size int) float64 {
	bd := bandwidthDown[index]
	for i := 0; i < size; i++ {
		id := fmt.Sprintf("%s%s", name, intToStr(i))
		nodeManager.addValidator(id, bd)
	}

	return bd * float64(size)
}

func initBeValidate(nodeManager *Manager, name string, index, size int) {
	for i := 0; i < size; i++ {
		id := fmt.Sprintf("%s%s", name, intToStr(i))
		nodeManager.addValidatableNode(id, bandwidthUp[index])
	}
}

func intToStr(i int) string {
	if i < 10 {
		return fmt.Sprintf("0000%d", i)
	}

	if i < 100 {
		return fmt.Sprintf("000%d", i)
	}
	if i < 1000 {
		return fmt.Sprintf("00%d", i)
	}

	if i < 10000 {
		return fmt.Sprintf("0%d", i)
	}

	return fmt.Sprintf("%d", i)
}
