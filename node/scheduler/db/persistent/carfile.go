package persistent

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

type CarfileDB struct {
	DB *sqlx.DB
}

func NewCarfileDB(db *sqlx.DB) *CarfileDB {
	return &CarfileDB{db}
}

// UpdateReplicaInfo update replica info
func (c *CarfileDB) UpdateReplicaInfo(cInfo *types.ReplicaInfo) error {
	query := fmt.Sprintf(`UPDATE %s SET end_time=NOW(), status=:status WHERE id=:id `, replicaInfoTable)
	_, err := c.DB.NamedExec(query, cInfo)
	return err
}

// SetReplicasFailed Set the status of replicas to failed
func (c *CarfileDB) SetReplicasFailed(hash string) error {
	query := fmt.Sprintf(`UPDATE %s SET end_time=NOW(), status=? WHERE carfile_hash=? AND status=?`, replicaInfoTable)
	_, err := c.DB.Exec(query, types.CacheStatusFailed, hash, types.CacheStatusDownloading)
	return err
}

// InsertOrUpdateReplicaInfo Insert or update replica info
func (c *CarfileDB) InsertOrUpdateReplicaInfo(infos []*types.ReplicaInfo) error {
	query := fmt.Sprintf(
		`INSERT INTO %s (id, carfile_hash, node_id, status, is_candidate) 
				VALUES (:id, :carfile_hash, :node_id, :status, :is_candidate) 
				ON DUPLICATE KEY UPDATE end_time=NOW(), status=VALUES(status)`, replicaInfoTable)

	_, err := c.DB.NamedExec(query, infos)

	return err
}

// UpdateOrCreateCarfileRecord update storage record info
func (c *CarfileDB) UpdateOrCreateCarfileRecord(info *types.CarfileRecordInfo) error {
	cmd := fmt.Sprintf(
		`INSERT INTO %s (carfile_hash, carfile_cid, state, edge_replica, candidate_replica, expiration, total_size, total_blocks, server_id) 
				VALUES (:carfile_hash, :carfile_cid, :state, :edge_replica, :candidate_replica, :expiration, :total_size, :total_blocks, :server_id) 
				ON DUPLICATE KEY UPDATE total_size=VALUES(total_size), total_blocks=VALUES(total_blocks), state=VALUES(state)`, carfileInfoTable)

	_, err := c.DB.NamedExec(cmd, info)
	return err
}

// CarfileRecordExisted Carfile record existed
func (c *CarfileDB) CarfileRecordExisted(hash string) (bool, error) {
	var count int
	cmd := fmt.Sprintf("SELECT count(carfile_hash) FROM %s WHERE carfile_hash=?", carfileInfoTable)
	err := c.DB.Get(&count, cmd, hash)
	return count > 0, err
}

// CarfileInfo get storage info with hash
func (c *CarfileDB) CarfileInfo(hash string) (*types.CarfileRecordInfo, error) {
	var info types.CarfileRecordInfo
	cmd := fmt.Sprintf("SELECT * FROM %s WHERE carfile_hash=?", carfileInfoTable)
	err := c.DB.Get(&info, cmd, hash)
	if err != nil {
		return nil, err
	}

	return &info, nil
}

// CarfileInfos get storage infos with hashs
func (c *CarfileDB) CarfileInfos(hashes []string) ([]*types.CarfileRecordInfo, error) {
	getCarfilesCmd := fmt.Sprintf(`SELECT * FROM %s WHERE carfile_hash in (?)`, carfileInfoTable)
	carfilesQuery, args, err := sqlx.In(getCarfilesCmd, hashes)
	if err != nil {
		return nil, err
	}

	tx, err := c.DB.Beginx()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	carfileRecords := make([]*types.CarfileRecordInfo, 0)

	carfilesQuery = c.DB.Rebind(carfilesQuery)
	tx.Select(&carfileRecords, carfilesQuery, args...)

	err = tx.Commit()

	return carfileRecords, err
}

// CountCarfiles count carfiles size
func (c *CarfileDB) CountCarfiles() (int, error) {
	var size int
	cmd := fmt.Sprintf("SELECT count(carfile_hash) FROM %s ;", carfileInfoTable)
	err := c.DB.Get(&size, cmd)
	if err != nil {
		return 0, err
	}
	return size, nil
}

// QueryCarfilesRows ...
func (c *CarfileDB) QueryCarfilesRows(ctx context.Context, limit, offset int, serverID dtypes.ServerID) (rows *sqlx.Rows, err error) {
	if limit == 0 {
		limit = loadCarfileInfoMaxCount
	}
	if limit > loadCarfileInfoMaxCount {
		limit = loadCarfileInfoMaxCount
	}

	cmd := fmt.Sprintf("SELECT * FROM %s WHERE state<>'Finalize' AND server_id=? order by carfile_hash asc LIMIT ? OFFSET ? ", carfileInfoTable)
	return c.DB.QueryxContext(ctx, cmd, serverID, limit, offset)
}

// CarfileRecordInfos get storage record infos
func (c *CarfileDB) CarfileRecordInfos(page int, status types.CacheStatus) (info *types.ListCarfileRecordRsp, err error) {
	num := loadCarfileInfoMaxCount

	info = &types.ListCarfileRecordRsp{}

	countCmd := fmt.Sprintf("SELECT count(carfile_hash) FROM %s ;", carfileInfoTable)
	selectCmd := fmt.Sprintf("SELECT * FROM %s ", carfileInfoTable)
	likeCondition := ""
	switch status {
	case types.CacheStatusFailed:
		likeCondition = "%" + "Failed"
		countCmd = fmt.Sprintf("SELECT count(carfile_hash) FROM %s WHERE state LIKE ?", carfileInfoTable)
		selectCmd = fmt.Sprintf("SELECT * FROM %s  WHERE state LIKE ?", carfileInfoTable)
	case types.CacheStatusDownloading:
		countCmd = fmt.Sprintf("SELECT count(carfile_hash) FROM %s WHERE state<>'Finalize';", carfileInfoTable)
		selectCmd = fmt.Sprintf("SELECT * FROM %s  WHERE state<>'Finalize' ", carfileInfoTable)
	case types.CacheStatusSucceeded:
		countCmd = fmt.Sprintf("SELECT count(carfile_hash) FROM %s WHERE state='Finalize';", carfileInfoTable)
		selectCmd = fmt.Sprintf("SELECT * FROM %s  WHERE state='Finalize' ", carfileInfoTable)
	}

	if likeCondition == "" {
		err = c.DB.Get(&info.Cids, countCmd)
	} else {
		err = c.DB.Get(&info.Cids, countCmd, likeCondition)
	}
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

	selectCmd += " order by carfile_hash asc LIMIT ?,?"
	// cmd := fmt.Sprintf(fmt.Sprintf("%s order by carfile_hash asc LIMIT %d,%d", selectCmd, (num * (page - 1)), num))
	if likeCondition == "" {
		err = c.DB.Select(&info.CarfileRecords, selectCmd, (num * (page - 1)), num)
	} else {
		err = c.DB.Select(&info.CarfileRecords, selectCmd, likeCondition, (num * (page - 1)), num)
	}

	return
}

// ReplicasSucceedCountByCarfile get edge and candidate succeed replicas
func (c *CarfileDB) ReplicasSucceedCountByCarfile(hash string) (edgeCount, candidateCont int64, err error) {
	cmd := fmt.Sprintf("SELECT count(if(is_candidate, true, null)) as candidateCont, count(if(is_candidate, false, null)) as edgeCount FROM %s WHERE carfile_hash=? AND status=? ", replicaInfoTable)
	err = c.DB.QueryRowx(cmd, hash, types.CacheStatusSucceeded).Scan(&candidateCont, &edgeCount)
	return
}

// CandidatesByCarfile get candidates by hash
func (c *CarfileDB) CandidatesByCarfile(hash string) ([]string, error) {
	var out []string
	query := fmt.Sprintf(`SELECT node_id FROM %s WHERE carfile_hash=? AND status=? AND is_candidate=?`,
		replicaInfoTable)

	if err := c.DB.Select(&out, query, hash, types.CacheStatusSucceeded, true); err != nil {
		return nil, err
	}

	return out, nil
}

// EdgesByCarfile get edges by hash
func (c *CarfileDB) EdgesByCarfile(hash string) ([]string, error) {
	var out []string
	query := fmt.Sprintf(`SELECT node_id FROM %s WHERE carfile_hash=? AND status=? AND is_candidate=?`,
		replicaInfoTable)

	if err := c.DB.Select(&out, query, hash, types.CacheStatusSucceeded, false); err != nil {
		return nil, err
	}

	return out, nil
}

// ReplicaInfosByCarfile get storage replica infos by hash
func (c *CarfileDB) ReplicaInfosByCarfile(hash string, needSucceed bool) ([]*types.ReplicaInfo, error) {
	var out []*types.ReplicaInfo
	if needSucceed {
		query := fmt.Sprintf(`SELECT * FROM %s WHERE carfile_hash=? AND status=?`, replicaInfoTable)

		if err := c.DB.Select(&out, query, hash, types.CacheStatusSucceeded); err != nil {
			return nil, err
		}
	} else {
		query := fmt.Sprintf(`SELECT * FROM %s WHERE carfile_hash=? `, replicaInfoTable)

		if err := c.DB.Select(&out, query, hash); err != nil {
			return nil, err
		}
	}

	return out, nil
}

// RandomCarfileFromNode Get a random carfile from the node
func (c *CarfileDB) RandomCarfileFromNode(nodeID string) (string, error) {
	query := fmt.Sprintf(`SELECT count(carfile_hash) FROM %s WHERE node_id=? AND status=?`, replicaInfoTable)

	var count int
	if err := c.DB.Get(&count, query, nodeID, types.CacheStatusSucceeded); err != nil {
		return "", err
	}

	if count < 1 {
		return "", xerrors.Errorf("node %s no cache", nodeID)
	}

	rand := rand.New(rand.NewSource(time.Now().UnixNano()))
	// rand count
	index := rand.Intn(count)

	var hashs []string
	cmd := fmt.Sprintf("SELECT carfile_hash FROM %s WHERE node_id=? AND status=? LIMIT %d,%d", replicaInfoTable, index, 1)
	if err := c.DB.Select(&hashs, cmd, nodeID, types.CacheStatusSucceeded); err != nil {
		return "", err
	}

	if len(hashs) > 0 {
		return hashs[0], nil
	}

	return "", nil
}

// ResetCarfileExpiration reset expiration time with storage record
func (c *CarfileDB) ResetCarfileExpiration(carfileHash string, eTime time.Time) error {
	cmd := fmt.Sprintf(`UPDATE %s SET expiration=? WHERE carfile_hash=?`, carfileInfoTable)
	_, err := c.DB.Exec(cmd, eTime, carfileHash)

	return err
}

// MinExpiration Get the minimum expiration time
func (c *CarfileDB) MinExpiration() (time.Time, error) {
	query := fmt.Sprintf(`SELECT MIN(expiration) FROM %s`, carfileInfoTable)

	var out time.Time
	if err := c.DB.Get(&out, query); err != nil {
		return out, err
	}

	return out, nil
}

// ExpiredCarfiles load all expired carfiles
func (c *CarfileDB) ExpiredCarfiles() ([]*types.CarfileRecordInfo, error) {
	query := fmt.Sprintf(`SELECT * FROM %s WHERE expiration <= NOW()`, carfileInfoTable)

	var out []*types.CarfileRecordInfo
	if err := c.DB.Select(&out, query); err != nil {
		return nil, err
	}

	return out, nil
}

// SucceedCachesCount get succeed caches count
func (c *CarfileDB) SucceedCachesCount() (int, error) {
	query := fmt.Sprintf(`SELECT count(carfile_hash) FROM %s WHERE status=?`, replicaInfoTable)

	var count int
	if err := c.DB.Get(&count, query, types.CacheStatusSucceeded); err != nil {
		return 0, err
	}

	return count, nil
}

// LoadReplicaInfo load replica info with id
func (c *CarfileDB) LoadReplicaInfo(id string) (*types.ReplicaInfo, error) {
	var cache types.ReplicaInfo
	query := fmt.Sprintf("SELECT * FROM %s WHERE id=? ", replicaInfoTable)
	if err := c.DB.Get(&cache, query, id); err != nil {
		return nil, err
	}

	return &cache, nil
}

// RemoveCarfileRecord remove storage
func (c *CarfileDB) RemoveCarfileRecord(carfileHash string) error {
	tx, err := c.DB.Beginx()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	// cache info
	cCmd := fmt.Sprintf(`DELETE FROM %s WHERE carfile_hash=? `, replicaInfoTable)
	tx.Exec(cCmd, carfileHash)

	// data info
	dCmd := fmt.Sprintf(`DELETE FROM %s WHERE carfile_hash=?`, carfileInfoTable)
	tx.Exec(dCmd, carfileHash)

	return tx.Commit()
}

// LoadCarfileRecordsWithNodes load carfile record hashs with nodes
func (c *CarfileDB) LoadCarfileRecordsWithNodes(nodeIDs []string) (hashs []string, err error) {
	tx, err := c.DB.Beginx()
	if err != nil {
		return
	}
	defer tx.Rollback()

	// get carfiles
	getCarfilesCmd := fmt.Sprintf(`select carfile_hash from %s WHERE node_id in (?) GROUP BY carfile_hash`, replicaInfoTable)
	carfilesQuery, args, err := sqlx.In(getCarfilesCmd, nodeIDs)
	if err != nil {
		return
	}

	carfilesQuery = c.DB.Rebind(carfilesQuery)
	tx.Select(&hashs, carfilesQuery, args...)

	err = tx.Commit()
	return
}

// RemoveReplicaInfoWithNodes remove replica info with nodes
func (c *CarfileDB) RemoveReplicaInfoWithNodes(nodeIDs []string) error {
	tx, err := c.DB.Beginx()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// remove cache
	cmd := fmt.Sprintf(`DELETE FROM %s WHERE node_id in (?)`, replicaInfoTable)
	query, args, err := sqlx.In(cmd, nodeIDs)
	if err != nil {
		return err
	}

	query = c.DB.Rebind(query)
	tx.Exec(query, args...)

	return tx.Commit()
}

// SetBlockDownloadInfo  download info
func (c *CarfileDB) SetBlockDownloadInfo(info *types.DownloadRecordInfo) error {
	query := fmt.Sprintf(
		`INSERT INTO %s (id, node_id, block_cid, carfile_cid, block_size, speed, reward, status, failed_reason, client_ip, created_time, complete_time) 
				VALUES (:id, :node_id, :block_cid, :carfile_cid, :block_size, :speed, :reward, :status, :failed_reason, :client_ip, :created_time, :complete_time) 
				ON DUPLICATE KEY UPDATE node_id=:node_id, speed=:speed, reward=:reward, status=:status, failed_reason=:failed_reason, complete_time=:complete_time`, blockDownloadInfo)

	_, err := c.DB.NamedExec(query, info)
	if err != nil {
		return err
	}

	return nil
}

// func (c *CarfileDB) GetBlockDownloadInfoByNodeID(nodeID string) ([]*types.DownloadRecordInfo, error) {
// 	query := fmt.Sprintf(`SELECT * FROM %s WHERE node_id = ? and TO_DAYS(created_time) >= TO_DAYS(NOW()) ORDER BY created_time DESC`, blockDownloadInfo)

// 	var out []*types.DownloadRecordInfo
// 	if err := c.DB.Select(&out, query, nodeID); err != nil {
// 		return nil, err
// 	}

// 	return out, nil
// }

func (c *CarfileDB) GetBlockDownloadInfoByID(id string) (*types.DownloadRecordInfo, error) {
	query := fmt.Sprintf(`SELECT * FROM %s WHERE id = ?`, blockDownloadInfo)

	var out []*types.DownloadRecordInfo
	if err := c.DB.Select(&out, query, id); err != nil {
		return nil, err
	}

	if len(out) > 0 {
		return out[0], nil
	}
	return nil, nil
}

func (c *CarfileDB) GetNodesByUserDownloadBlockIn(minute int) ([]string, error) {
	starTime := time.Now().Add(time.Duration(minute) * time.Minute * -1)

	query := fmt.Sprintf(`SELECT node_id FROM %s WHERE complete_time > ? group by node_id`, blockDownloadInfo)

	var out []string
	if err := c.DB.Select(&out, query, starTime); err != nil {
		return nil, err
	}

	return out, nil
}

func (c *CarfileDB) GetCacheInfosWithNode(nodeID string, index, count int) (info *types.NodeCacheRsp, err error) {
	info = &types.NodeCacheRsp{}

	cmd := fmt.Sprintf("SELECT count(id) FROM %s WHERE node_id=?", replicaInfoTable)
	err = c.DB.Get(&info.TotalCount, cmd, nodeID)
	if err != nil {
		return
	}

	cmd = fmt.Sprintf("SELECT carfile_hash,status FROM %s WHERE node_id=? order by id asc LIMIT %d,%d", replicaInfoTable, index, count)
	if err = c.DB.Select(&info.Caches, cmd, nodeID); err != nil {
		return
	}

	return
}

func (c *CarfileDB) GetBlockDownloadInfos(nodeID string, startTime time.Time, endTime time.Time, cursor, count int) ([]types.DownloadRecordInfo, int64, error) {
	query := fmt.Sprintf(`SELECT * FROM %s WHERE node_id = ? and created_time between ? and ? limit ?,?`, blockDownloadInfo)

	var total int64
	countSQL := fmt.Sprintf(`SELECT count(*) FROM %s WHERE node_id = ? and created_time between ? and ?`, blockDownloadInfo)
	if err := c.DB.Get(&total, countSQL, nodeID, startTime, endTime); err != nil {
		return nil, 0, err
	}

	if count > loadBlockDownloadMaxCount {
		count = loadBlockDownloadMaxCount
	}

	var out []types.DownloadRecordInfo
	if err := c.DB.Select(&out, query, nodeID, startTime, endTime, cursor, count); err != nil {
		return nil, 0, err
	}

	return out, total, nil
}

func (c *CarfileDB) CarfileReplicaList(startTime time.Time, endTime time.Time, cursor, count int) (*types.ListCarfileReplicaRsp, error) {
	var total int64
	countSQL := fmt.Sprintf(`SELECT count(*) FROM %s WHERE end_time between ? and ?`, replicaInfoTable)
	if err := c.DB.Get(&total, countSQL, startTime, endTime); err != nil {
		return nil, err
	}

	if count > loadReplicaInfoMaxCount {
		count = loadReplicaInfoMaxCount
	}

	query := fmt.Sprintf(`SELECT * FROM %s WHERE end_time between ? and ? limit ?,?`, replicaInfoTable)

	var out []*types.ReplicaInfo
	if err := c.DB.Select(&out, query, startTime, endTime, cursor, count); err != nil {
		return nil, err
	}

	return &types.ListCarfileReplicaRsp{Datas: out, Total: total}, nil
}

// PushCarfileToWaitList waiting data list
func (c *CarfileDB) PushCarfileToWaitList(info *types.CacheCarfileInfo) error {
	query := fmt.Sprintf(
		`INSERT INTO %s (carfile_hash, carfile_cid, replicas, node_id, expiration, server_id) 
				VALUES (:carfile_hash, :carfile_cid, :replicas, :node_id, :expiration, :server_id) 
				ON DUPLICATE KEY UPDATE carfile_hash=:carfile_hash, carfile_cid=:carfile_cid, replicas=:replicas, node_id=:node_id, 
				expiration=:expiration, server_id=:server_id`, waitingCarfileTable)

	_, err := c.DB.NamedExec(query, info)
	return err
}

// LoadWaitCarfiles load
func (c *CarfileDB) LoadWaitCarfiles(serverID dtypes.ServerID) (*types.CacheCarfileInfo, error) {
	sQuery := fmt.Sprintf(`SELECT * FROM %s WHERE server_id=? order by id asc limit ?`, waitingCarfileTable)

	info := &types.CacheCarfileInfo{}
	err := c.DB.Get(info, sQuery, serverID, 1)
	if err != nil {
		return nil, err
	}

	return info, nil
}

// RemoveWaitCarfile remove
func (c *CarfileDB) RemoveWaitCarfile(id string) error {
	query := fmt.Sprintf(`DELETE FROM %s WHERE id=?`, waitingCarfileTable)
	_, err := c.DB.Exec(query, id)
	return err
}

// GetCachingCarfiles ...
func (c *CarfileDB) GetCachingCarfiles(serverID dtypes.ServerID) ([]string, error) {
	sQuery := fmt.Sprintf(`SELECT carfile_hash FROM %s WHERE server_id=? GROUP BY carfile_hash`, downloadingTable)

	var out []string
	if err := c.DB.Select(&out, sQuery, serverID); err != nil {
		return nil, err
	}
	return out, nil
}

// ReplicaTasksStart ...
func (c *CarfileDB) ReplicaTasksStart(serverID dtypes.ServerID, hash string, nodeIDs []string) error {
	tx, err := c.DB.Beginx()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	for _, nodeID := range nodeIDs {
		sQuery := fmt.Sprintf(`INSERT INTO %s (carfile_hash, node_id, server_id) VALUES (?, ?, ?)`, downloadingTable)
		tx.Exec(sQuery, hash, nodeID, serverID)
	}

	return tx.Commit()
}

// ReplicaTasksEnd ...
func (c *CarfileDB) ReplicaTasksEnd(serverID dtypes.ServerID, hash string, nodeIDs []string) (bool, error) {
	dQuery := fmt.Sprintf("DELETE FROM %s WHERE server_id=? AND carfile_hash=? AND node_id in (?) ", downloadingTable)
	query, args, err := sqlx.In(dQuery, serverID, hash, nodeIDs)
	if err != nil {
		return false, err
	}

	// cache info
	query = c.DB.Rebind(query)
	_, err = c.DB.Exec(query, args...)
	if err != nil {
		return false, err
	}

	var count int
	sQuery := fmt.Sprintf("SELECT count(*) FROM %s WHERE carfile_hash=? AND server_id=?", downloadingTable)
	err = c.DB.Get(&count, sQuery, hash, serverID)
	if err != nil {
		return false, err
	}

	return count == 0, nil
}
