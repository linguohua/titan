package db

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/linguohua/titan/api/types"

	"github.com/jmoiron/sqlx"
	"github.com/linguohua/titan/node/modules/dtypes"
	"golang.org/x/xerrors"
)

// UpdateUnfinishedReplicaInfo update unfinished replica info , return an error if the replica is finished
func (n *SQLDB) UpdateUnfinishedReplicaInfo(cInfo *types.ReplicaInfo) error {
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

// UpdateStatusOfUnfinishedReplicas update status of unfinished carfile replicas
func (n *SQLDB) UpdateStatusOfUnfinishedReplicas(hash string, status types.CacheStatus) error {
	query := fmt.Sprintf(`UPDATE %s SET end_time=NOW(), status=? WHERE carfile_hash=? AND (status=? or status=?)`, replicaInfoTable)
	_, err := n.db.Exec(query, status, hash, types.CacheStatusCaching, types.CacheStatusWaiting)

	return err
}

// BatchUpsertReplicas Insert or update replicas info
func (n *SQLDB) BatchUpsertReplicas(infos []*types.ReplicaInfo) error {
	query := fmt.Sprintf(
		`INSERT INTO %s (id, carfile_hash, node_id, status, is_candidate) 
				VALUES (:id, :carfile_hash, :node_id, :status, :is_candidate) 
				ON DUPLICATE KEY UPDATE status=VALUES(status)`, replicaInfoTable)

	_, err := n.db.NamedExec(query, infos)

	return err
}

// UpsertCarfileRecord Insert or update carfile record info
func (n *SQLDB) UpsertCarfileRecord(info *types.CarfileRecordInfo) error {
	cmd := fmt.Sprintf(
		`INSERT INTO %s (carfile_hash, carfile_cid, state, edge_replicas, candidate_replicas, expiration, total_size, total_blocks, server_id, end_time) 
				VALUES (:carfile_hash, :carfile_cid, :state, :edge_replicas, :candidate_replicas, :expiration, :total_size, :total_blocks, :server_id, NOW()) 
				ON DUPLICATE KEY UPDATE total_size=VALUES(total_size), total_blocks=VALUES(total_blocks), state=VALUES(state), end_time=NOW()`, carfileRecordTable)

	_, err := n.db.NamedExec(cmd, info)
	return err
}

// LoadCarfileRecordInfo load carfile record of carfile
func (n *SQLDB) LoadCarfileRecordInfo(hash string) (*types.CarfileRecordInfo, error) {
	var info types.CarfileRecordInfo
	cmd := fmt.Sprintf("SELECT * FROM %s WHERE carfile_hash=?", carfileRecordTable)
	err := n.db.Get(&info, cmd, hash)

	return &info, err
}

// LoadCarfileRecords load carfile record infos
func (n *SQLDB) LoadCarfileRecords(statuses []string, limit, offset int, serverID dtypes.ServerID) (*sqlx.Rows, error) {
	if limit > loadCarfileRecordsLimit || limit == 0 {
		limit = loadCarfileRecordsLimit
	}

	selectCmd := fmt.Sprintf(`SELECT * FROM %s WHERE state in (?) AND server_id=? order by carfile_hash asc LIMIT ? OFFSET ?`, carfileRecordTable)
	selectQuery, args, err := sqlx.In(selectCmd, statuses, serverID, limit, offset)
	if err != nil {
		return nil, err
	}

	selectQuery = n.db.Rebind(selectQuery)
	return n.db.QueryxContext(context.Background(), selectQuery, args...)
}

// LoadSucceededReplicas load succeed replica nodeID by hash
func (n *SQLDB) LoadSucceededReplicas(hash string, nType types.NodeType) ([]string, error) {
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

// LoadReplicaInfosOfCarfile load carfile replica infos of hash
func (n *SQLDB) LoadReplicaInfosOfCarfile(hash string, needSucceeded bool) ([]*types.ReplicaInfo, error) {
	var out []*types.ReplicaInfo
	if needSucceeded {
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

// LoadReplicaCountOfNode load succeeded replica count of node
func (n *SQLDB) LoadReplicaCountOfNode(nodeID string) (int, error) {
	query := fmt.Sprintf(`SELECT count(carfile_hash) FROM %s WHERE node_id=? AND status=?`, replicaInfoTable)

	var count int
	err := n.db.Get(&count, query, nodeID, types.CacheStatusSucceeded)

	return count, err
}

// LoadCarfileHashesOfNode load a carfile of the node
func (n *SQLDB) LoadCarfileHashesOfNode(nodeID string, limit, offset int) ([]string, error) {
	var hashes []string
	cmd := fmt.Sprintf("SELECT carfile_hash FROM %s WHERE node_id=? AND status=? LIMIT %d OFFSET %d", replicaInfoTable, limit, offset)
	if err := n.db.Select(&hashes, cmd, nodeID, types.CacheStatusSucceeded); err != nil {
		return nil, err
	}

	return hashes, nil
}

// UpdateCarfileRecordExpiration reset carfile record expiration time
func (n *SQLDB) UpdateCarfileRecordExpiration(carfileHash string, eTime time.Time) error {
	cmd := fmt.Sprintf(`UPDATE %s SET expiration=? WHERE carfile_hash=?`, carfileRecordTable)
	_, err := n.db.Exec(cmd, eTime, carfileHash)

	return err
}

// LoadMinExpirationOfCarfileRecords Get the minimum expiration time of carfile records
func (n *SQLDB) LoadMinExpirationOfCarfileRecords() (time.Time, error) {
	query := fmt.Sprintf(`SELECT MIN(expiration) FROM %s`, carfileRecordTable)

	var out time.Time
	if err := n.db.Get(&out, query); err != nil {
		return out, err
	}

	return out, nil
}

// LoadExpiredCarfileRecords load all expired carfile records
func (n *SQLDB) LoadExpiredCarfileRecords() ([]*types.CarfileRecordInfo, error) {
	query := fmt.Sprintf(`SELECT * FROM %s WHERE expiration <= NOW()`, carfileRecordTable)

	var out []*types.CarfileRecordInfo
	if err := n.db.Select(&out, query); err != nil {
		return nil, err
	}

	return out, nil
}

// LoadCachingNodes load unfinished nodes for carfile
func (n *SQLDB) LoadCachingNodes(hash string) ([]string, error) {
	var nodes []string
	query := fmt.Sprintf(`SELECT node_id FROM %s WHERE carfile_hash=? AND (status=? or status=?)`, replicaInfoTable)
	err := n.db.Select(&nodes, query, hash, types.CacheStatusCaching, types.CacheStatusWaiting)
	return nodes, err
}

// RemoveCarfileRecord remove carfile record
func (n *SQLDB) RemoveCarfileRecord(carfileHash string) error {
	tx, err := n.db.Beginx()
	if err != nil {
		return err
	}

	defer func() {
		err = tx.Rollback()
		if err != nil && err != sql.ErrTxDone {
			log.Errorf("RemoveCarfileRecord Rollback err:%s", err.Error())
		}
	}()

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

// LoadCarfileHashesOfNodes load carfile hashes of nodes
func (n *SQLDB) LoadCarfileHashesOfNodes(nodeIDs []string) (hashes []string, err error) {
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
func (n *SQLDB) RemoveReplicaInfoOfNodes(nodeIDs []string) error {
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
func (n *SQLDB) LoadReplicaInfosOfNode(nodeID string, index, count int) (info *types.NodeReplicaRsp, err error) {
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

// LoadReplicaInfos load replicas info
func (n *SQLDB) LoadReplicaInfos(startTime time.Time, endTime time.Time, cursor, count int) (*types.ListCarfileReplicaRsp, error) {
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
