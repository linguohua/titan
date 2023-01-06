package validate

import (
	"math"
	"sort"
)

// GetAverageArray divide the array into n arrays, and the sum of each array should be as close as possible
func GetAverageArray(validatedList []validatedDeviceInfo, arrNum int) [][]validatedDeviceInfo {
	avgArrays := make([][]validatedDeviceInfo, 0)
	if len(validatedList) == 0 || len(validatedList) < arrNum {
		return avgArrays
	}

	// calculate Average
	var sum, mean float64
	for _, validated := range validatedList {
		sum += validated.bandwidth
	}

	mean = sum / float64(arrNum)

	// sort in reverse order
	sort.Slice(validatedList, func(i, j int) bool {
		return validatedList[i].bandwidth > validatedList[j].bandwidth
	})

	for cnt := 0; cnt < arrNum; cnt++ {
		var arr []validatedDeviceInfo
		if cnt == arrNum-1 {
			// the last group returns all the remaining numbers of the array
			avgArrays = append(avgArrays, validatedList)
			break
		}
		// if the maximum number max>=mean, this number is a separate group
		if len(validatedList) > 0 && validatedList[0].bandwidth >= mean {
			arr = []validatedDeviceInfo{validatedList[0]}
			avgArrays = append(avgArrays, arr)
			sum = sum - validatedList[0].bandwidth

			// recalculate the average value of the remaining partition
			mean = sum / float64(arrNum-len(avgArrays))
		} else {
			// otherwise, look for a set of data
			arr, _ = getList(validatedList, mean, math.Pow(mean, 2))
			avgArrays = append(avgArrays, arr)
		}
		// remove the data that has formed a group from the original array, and prepare to find the next group of data
		validatedList = removeFromList(validatedList, arr)
	}

	return avgArrays
}

func removeFromList(originalList []validatedDeviceInfo, removeNums []validatedDeviceInfo) []validatedDeviceInfo {
	res := make([]validatedDeviceInfo, 0)
	from := 0
	for _, remove := range removeNums {
		for i := from; i < len(originalList); i++ {
			if originalList[i] == remove {
				res = append(res, originalList[from:i]...)
				from = i + 1
				break
			}
		}
	}
	res = append(res, originalList[from:]...)
	return res
}

func getList(arr []validatedDeviceInfo, delta, distance float64) ([]validatedDeviceInfo, float64) {
	res := make([]validatedDeviceInfo, 0)
	if len(arr) == 0 {
		return res, -1
	}

	for i := 0; i < len(arr)-1; i++ {
		switch true {
		case delta == arr[i].bandwidth:
			res = append(res, arr[i])
			return res, 0
		case delta < arr[i].bandwidth:
			continue
		case delta > arr[i].bandwidth:
			if i == 0 {
				res = append(res, arr[i])
				delta = delta - arr[i].bandwidth
				distance = math.Pow(delta, 2)
				tmp, d := getList(arr[i+1:], delta, distance)
				res = append(res, tmp...)
				return res, d
			} else {
				dis1 := math.Pow(arr[i-1].bandwidth-delta, 2)
				dis2 := math.Pow(delta-arr[i].bandwidth, 2)
				if dis1 > dis2 {
					res = append(res, arr[i])
					delta = delta - arr[i].bandwidth
					tmp, d := getList(arr[i+1:], delta, dis2)
					res = append(res, tmp...)
					return res, d
				} else {
					tmp, d := getList(arr[i:], delta, dis2)
					if dis1 > d {
						res = append(res, tmp...)
						return res, d
					}

					res = append(res, arr[i-1])
					return res, dis1
				}
			}
		}
	}

	dis := math.Pow(delta-arr[len(arr)-1].bandwidth, 2)

	if dis < distance {
		return arr[len(arr)-1:], dis
	}

	return make([]validatedDeviceInfo, 0), -1
}
