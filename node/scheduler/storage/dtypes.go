package storage

import (
	"time"

	"github.com/linguohua/titan/api/types"
)

// CarfileID is an identifier for a carfile.
type CarfileID string

func (c CarfileID) String() string {
	return string(c)
}

type Log struct {
	Timestamp uint64
	Trace     string // for errors

	Message string

	// additional data (Event info)
	Kind string
}

type CarfileInfo struct {
	ID          string
	State       CarfileState `db:"state"`
	CarfileCID  CarfileID    `db:"carfile_cid"`
	CarfileHash string       `db:"carfile_hash"`
	Replicas    int          `db:"replicas"`
	NodeID      string       `db:"node_id"`
	ServerID    string       `db:"server_id"`
	Size        int64        `db:"size"`
	Blocks      int64        `db:"blocks"`
	CreatedAt   time.Time    `db:"created_at"`
	Expiration  time.Time    `db:"expiration"`

	Log                 []Log
	CandidateStoreFails int
	EdgeStoreFails      int
}

func (state *CarfileInfo) toCacheCarfileInfo() *types.CacheCarfileInfo {
	return &types.CacheCarfileInfo{
		CarfileCid:     string(state.CarfileCID),
		CarfileHash:    state.CarfileHash,
		Replicas:       state.Replicas,
		NodeID:         state.NodeID,
		ServerID:       state.ServerID,
		ExpirationTime: state.Expiration,
	}
}

func fromCarfileInfo(info *types.CacheCarfileInfo) *CarfileInfo {
	return &CarfileInfo{
		CarfileCID:  CarfileID(info.CarfileCid),
		CarfileHash: info.CarfileHash,
		Replicas:    info.Replicas,
		NodeID:      info.NodeID,
		ServerID:    info.ServerID,
		Expiration:  info.ExpirationTime,
	}
}
