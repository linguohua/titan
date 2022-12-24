package block

import "sync"

type carfile struct {
	carfileHash string
	delayReqs   []*delayReq
	lock        *sync.Mutex
}

func (carfile *carfile) removeReq(len int) []*delayReq {
	carfile.lock.Lock()
	defer carfile.lock.Unlock()

	reqs := carfile.delayReqs[:len]
	carfile.delayReqs = carfile.delayReqs[len:]
	return reqs
}

func (carfile *carfile) addReq(delayReqs []*delayReq) {
	carfile.lock.Lock()
	defer carfile.lock.Unlock()

	carfile.delayReqs = append(carfile.delayReqs, delayReqs...)
}
