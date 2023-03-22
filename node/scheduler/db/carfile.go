package db

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/linguohua/titan/api/types"

	"github.com/jmoiron/sqlx"
	"github.com/linguohua/titan/node/modules/dtypes"
	"golang.org/x/xerrors"
)

// UpdateReplicaInfo update replica info
func (n *SqlDB) UpdateReplicaInfo(cInfo *types.ReplicaInfo) error {
	query := fmt.Sprintf(`UPDATE %s SET end_time=NOW(), status=?, done_size=? WHERE id=? AND (status=? or status=?)`, replicaInfoTable)
	result, err := n.db.Exec(query, cInfo.Status, cInfo.DoneSize, cInfo.ID, types.CacheStatusCaching, types.CacheStatusWaiting)
	if err != nil {
		return err
	}

	r, err := result.RowsAffected()
	if err != nil {
		return err
	}

	if r < 1 {
		return xerrors.New("nothing to update")
	}

	return nil
}

// UpdateTimeoutOfCarfileReplicas set timeout status of carfile replicas
func (n *SqlDB) UpdateTimeoutOfCarfileReplicas(hash string) error {
	query := fmt.Sprintf(`UPDATE %s SET end_time=NOW(), status=? WHERE carfile_hash=? AND (status=? or status=?)`, replicaInfoTable)
	_, err := n.db.Exec(query, types.CacheStatusFailed, hash, types.CacheStatusCaching, types.CacheStatusWaiting)

	return err
}

// UpsertReplicasInfo Insert or update replicas info
func (n *SqlDB) UpsertReplicasInfo(infos []*types.ReplicaInfo) error {
	query := fmt.Sprintf(
		`INSERT INTO %s (id, carfile_hash, node_id, status, is_candidate) 
				VALUES (:id, :carfile_hash, :node_id, :status, :is_candidate) 
				ON DUPLICATE KEY UPDATE status=VALUES(status)`, replicaInfoTable)

	_, err := n.db.NamedExec(query, infos)

	return err
}

// UpsertCarfileRecord update carfile record info
func (n *SqlDB) UpsertCarfileRecord(info *types.CarfileRecordInfo) error {
	cmd := fmt.Sprintf(
		`INSERT INTO %s (carfile_hash, carfile_cid, state, edge_replicas, candidate_replicas, expiration, total_size, total_blocks, server_id, end_time) 
				VALUES (:carfile_hash, :carfile_cid, :state, :edge_replicas, :candidate_replicas, :expiration, :total_size, :total_blocks, :server_id, NOW()) 
				ON DUPLICATE KEY UPDATE total_size=VALUES(total_size), total_blocks=VALUES(total_blocks), state=VALUES(state), end_time=NOW()`, carfileRecordTable)

	_, err := n.db.NamedExec(cmd, info)
	return err
}

// LoadCarfileRecordInfo get carfile record of carfile
func (n *SqlDB) LoadCarfileRecordInfo(hash string) (*types.CarfileRecordInfo, error) {
	var info types.CarfileRecordInfo
	cmd := fmt.Sprintf("SELECT * FROM %s WHERE carfile_hash=?", carfileRecordTable)
	err := n.db.Get(&info, cmd, hash)
	if err != nil {
		return nil, err
	}

	return &info, nil
}

// LoadUnfinishedCarfiles list Unfinished carfile record
func (n *SqlDB) LoadUnfinishedCarfiles(ctx context.Context, limit, offset int, serverID dtypes.ServerID) (rows *sqlx.Rows, err error) {
	if limit == 0 {
		limit = loadCarfileRecordsLimit
	}
	if limit > loadCarfileRecordsLimit {
		limit = loadCarfileRecordsLimit
	}

	cmd := fmt.Sprintf("SELECT * FROM %s WHERE state<>'Finished' AND server_id=? order by carfile_hash asc LIMIT ? OFFSET ? ", carfileRecordTable)
	return n.db.QueryxContext(ctx, cmd, serverID, limit, offset)
}

// LoadCarfileRecords get carfile record infos
func (n *SqlDB) LoadCarfileRecords(page int, statuses []string) (info *types.ListCarfileRecordRsp, err error) {
	num := loadCarfileRecordsLimit
	info = &types.ListCarfileRecordRsp{}

	countCmd := fmt.Sprintf(`SELECT count(carfile_hash) FROM %s WHERE state in (?) `, carfileRecordTable)
	countQuery, args, err := sqlx.In(countCmd, statuses)
	if err != nil {
		return
	}

	countQuery = n.db.Rebind(countQuery)
	err = n.db.Get(&info.Cids, countQuery, args...)
	if err != nil {
		return
	}

	info.TotalPage = info.Cids / num
	if info.Cids%num > 0 {
		info.TotalPage++
	}

	if info.TotalPage == 0 {
		return
	}

	if page > info.TotalPage {
		page = info.TotalPage
	}
	info.Page = page

	selectCmd := fmt.Sprintf(`SELECT * FROM %s WHERE state in (?) order by carfile_hash asc LIMIT ?,?`, carfileRecordTable)
	selectQuery, args, err := sqlx.In(selectCmd, statuses, num*(page-1), num)
	if err != nil {
		return
	}

	selectQuery = n.db.Rebind(selectQuery)
	err = n.db.Select(&info.CarfileRecords, selectQuery, args...)
	if err != nil {
		return
	}

	return
}

// LoadSucceededReplicas load succeed replica nodeID by hash
func (n *SqlDB) LoadSucceededReplicas(hash string, nType types.NodeType) ([]string, error) {
	isC := false

	switch nType {
	case types.NodeCandidate:
		isC = true
	case types.NodeEdge:
	default:
		return nil, xerrors.Errorf("node type is err:%d", nType)
	}

	var out []string
	query := fmt.Sprintf(`SELECT node_id FROM %s WHERE carfile_hash=? AND status=? AND is_candidate=?`,
		replicaInfoTable)

	if err := n.db.Select(&out, query, hash, types.CacheStatusSucceeded, isC); err != nil {
		return nil, err
	}

	return out, nil
}

// LoadReplicaInfos load carfile replica infos by hash
func (n *SqlDB) LoadReplicaInfos(hash string, needSucceed bool) ([]*types.ReplicaInfo, error) {
	var out []*types.ReplicaInfo
	if needSucceed {
		query := fmt.Sprintf(`SELECT * FROM %s WHERE carfile_hash=? AND status=?`, replicaInfoTable)

		if err := n.db.Select(&out, query, hash, types.CacheStatusSucceeded); err != nil {
			return nil, err
		}
	} else {
		query := fmt.Sprintf(`SELECT * FROM %s WHERE carfile_hash=? `, replicaInfoTable)

		if err := n.db.Select(&out, query, hash); err != nil {
			return nil, err
		}
	}

	return out, nil
}

// LoadRandomCarfileForNode Get a random carfile for the node
func (n *SqlDB) LoadRandomCarfileForNode(nodeID string) (string, error) {
	query := fmt.Sprintf(`SELECT count(carfile_hash) FROM %s WHERE node_id=? AND status=?`, replicaInfoTable)

	var count int
	if err := n.db.Get(&count, query, nodeID, types.CacheStatusSucceeded); err != nil {
		return "", err
	}

	if count < 1 {
		return "", xerrors.Errorf("node %s no cache", nodeID)
	}

	rand := rand.New(rand.NewSource(time.Now().UnixNano()))
	// rand count
	index := rand.Intn(count)

	var hashes []string
	cmd := fmt.Sprintf("SELECT carfile_hash FROM %s WHERE node_id=? AND status=? LIMIT %d,%d", replicaInfoTable, index, 1)
	if err := n.db.Select(&hashes, cmd, nodeID, types.CacheStatusSucceeded); err != nil {
		return "", err
	}

	if len(hashes) > 0 {
		return hashes[0], nil
	}

	return "", nil
}

// UpdateCarfileRecordExpiration reset carfile record expiration time
func (n *SqlDB) UpdateCarfileRecordExpiration(carfileHash string, eTime time.Time) error {
	cmd := fmt.Sprintf(`UPDATE %s SET expiration=? WHERE carfile_hash=?`, carfileRecordTable)
	_, err := n.db.Exec(cmd, eTime, carfileHash)

	return err
}

// GetMinExpirationOfCarfiles Get the minimum expiration time of carfile records
func (n *SqlDB) GetMinExpirationOfCarfiles() (time.Time, error) {
	query := fmt.Sprintf(`SELECT MIN(expiration) FROM %s`, carfileRecordTable)

	var out time.Time
	if err := n.db.Get(&out, query); err != nil {
		return out, err
	}

	return out, nil
}

// LoadExpiredCarfileCarfiles load all expired carfiles
func (n *SqlDB) LoadExpiredCarfileCarfiles() ([]*types.CarfileRecordInfo, error) {
	query := fmt.Sprintf(`SELECT * FROM %s WHERE expiration <= NOW()`, carfileRecordTable)

	var out []*types.CarfileRecordInfo
	if err := n.db.Select(&out, query); err != nil {
		return nil, err
	}

	return out, nil
}

// LoadCachingNodes load unfinished nodes for carfile
func (n *SqlDB) LoadCachingNodes(hash string) ([]string, error) {
	var nodes []string
	query := fmt.Sprintf(`SELECT node_id FROM %s WHERE carfile_hash=? AND (status=? or status=?)`, replicaInfoTable)
	err := n.db.Select(&nodes, query, hash, types.CacheStatusCaching, types.CacheStatusWaiting)
	return nodes, err
}

// RemoveCarfileRecord remove carfile record
func (n *SqlDB) RemoveCarfileRecord(carfileHash string) error {
	tx, err := n.db.Beginx()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	// cache info
	cCmd := fmt.Sprintf(`DELETE FROM %s WHERE carfile_hash=? `, replicaInfoTable)
	_, err = tx.Exec(cCmd, carfileHash)
	if err != nil {
		return err
	}

	// data info
	dCmd := fmt.Sprintf(`DELETE FROM %s WHERE carfile_hash=?`, carfileRecordTable)
	_, err = tx.Exec(dCmd, carfileHash)
	if err != nil {
		return err
	}

	return tx.Commit()
}

// LoadCarfileRecordsOfNodes load carfile record hashes of nodes
func (n *SqlDB) LoadCarfileRecordsOfNodes(nodeIDs []string) (hashes []string, err error) {
	// get carfiles
	getCarfilesCmd := fmt.Sprintf(`select carfile_hash from %s WHERE node_id in (?) GROUP BY carfile_hash`, replicaInfoTable)
	carfilesQuery, args, err := sqlx.In(getCarfilesCmd, nodeIDs)
	if err != nil {
		return
	}

	carfilesQuery = n.db.Rebind(carfilesQuery)
	err = n.db.Select(&hashes, carfilesQuery, args...)

	return
}

// RemoveReplicaInfoOfNodes remove replica info of nodes
func (n *SqlDB) RemoveReplicaInfoOfNodes(nodeIDs []string) error {
	// remove cache
	cmd := fmt.Sprintf(`DELETE FROM %s WHERE node_id in (?)`, replicaInfoTable)
	query, args, err := sqlx.In(cmd, nodeIDs)
	if err != nil {
		return err
	}

	query = n.db.Rebind(query)
	_, err = n.db.Exec(query, args...)

	return err
}

// LoadReplicaInfosOfNode load node replica infos
func (n *SqlDB) LoadReplicaInfosOfNode(nodeID string, index, count int) (info *types.NodeReplicaRsp, err error) {
	info = &types.NodeReplicaRsp{}

	cmd := fmt.Sprintf("SELECT count(id) FROM %s WHERE node_id=?", replicaInfoTable)
	err = n.db.Get(&info.TotalCount, cmd, nodeID)
	if err != nil {
		return
	}

	cmd = fmt.Sprintf("SELECT carfile_hash,status FROM %s WHERE node_id=? order by id asc LIMIT %d,%d", replicaInfoTable, index, count)
	if err = n.db.Select(&info.Replica, cmd, nodeID); err != nil {
		return
	}

	return
}

func (n *SqlDB) LoadBlockDownloadInfos(nodeID string, startTime time.Time, endTime time.Time, cursor, count int) ([]types.DownloadRecordInfo, int64, error) {
	query := fmt.Sprintf(`SELECT * FROM %s WHERE node_id = ? and created_time between ? and ? limit ?,?`, blockDownloadTable)

	var total int64
	countSQL := fmt.Sprintf(`SELECT count(*) FROM %s WHERE node_id = ? and created_time between ? and ?`, blockDownloadTable)
	if err := n.db.Get(&total, countSQL, nodeID, startTime, endTime); err != nil {
		return nil, 0, err
	}

	if count > loadBlockDownloadsLimit {
		count = loadBlockDownloadsLimit
	}

	var out []types.DownloadRecordInfo
	if err := n.db.Select(&out, query, nodeID, startTime, endTime, cursor, count); err != nil {
		return nil, 0, err
	}

	return out, total, nil
}

func (n *SqlDB) LoadCarfileReplicas(startTime time.Time, endTime time.Time, cursor, count int) (*types.ListCarfileReplicaRsp, error) {
	var total int64
	countSQL := fmt.Sprintf(`SELECT count(*) FROM %s WHERE end_time between ? and ?`, replicaInfoTable)
	if err := n.db.Get(&total, countSQL, startTime, endTime); err != nil {
		return nil, err
	}

	if count > loadReplicaInfosLimit {
		count = loadReplicaInfosLimit
	}

	query := fmt.Sprintf(`SELECT * FROM %s WHERE end_time between ? and ? limit ?,?`, replicaInfoTable)

	var out []*types.ReplicaInfo
	if err := n.db.Select(&out, query, startTime, endTime, cursor, count); err != nil {
		return nil, err
	}

	return &types.ListCarfileReplicaRsp{Datas: out, Total: total}, nil
}
