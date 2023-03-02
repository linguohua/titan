package persistent

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/linguohua/titan/api"
	"golang.org/x/xerrors"
)

type CarfileDB struct {
	db *sqlx.DB
}

func NewCarfileDB(db *sqlx.DB) *CarfileDB {
	return &CarfileDB{db}
}

// CreateCarfileReplicaInfo Create replica info
func (c *CarfileDB) CreateCarfileReplicaInfo(cInfo *api.ReplicaInfo) error {
	cmd := fmt.Sprintf("INSERT INTO %s (id, carfile_hash, device_id, status, is_candidate) VALUES (:id, :carfile_hash, :device_id, :status, :is_candidate)", replicaInfoTable)
	_, err := c.db.NamedExec(cmd, cInfo)
	return err
}

// UpdateCarfileReplicaStatus Update
func (c *CarfileDB) UpdateCarfileReplicaStatus(hash string, deviceIDs []string, status api.CacheStatus) error {
	tx := c.db.MustBegin()

	cmd := fmt.Sprintf("UPDATE %s SET status=? WHERE carfile_hash=? AND device_id in (?) ", replicaInfoTable)
	query, args, err := sqlx.In(cmd, status, hash, deviceIDs)
	if err != nil {
		return err
	}

	// cache info
	query = c.db.Rebind(query)
	tx.MustExec(query, args...)

	err = tx.Commit()
	if err != nil {
		err = tx.Rollback()
		return err
	}

	return nil
}

// UpdateCarfileReplicaInfo update replica info
func (c *CarfileDB) UpdateCarfileReplicaInfo(cInfo *api.ReplicaInfo) error {
	cmd := fmt.Sprintf("UPDATE %s SET status=:status,end_time=:end_time WHERE id=:id", replicaInfoTable)
	_, err := c.db.NamedExec(cmd, cInfo)

	return err
}

// UpdateCarfileRecordCachesInfo update carfile info
func (c *CarfileDB) UpdateCarfileRecordCachesInfo(dInfo *api.CarfileRecordInfo) error {
	// update
	cmd := fmt.Sprintf("UPDATE %s SET total_size=:total_size,total_blocks=:total_blocks,end_time=NOW(),replica=:replica,expired_time=:expired_time WHERE carfile_hash=:carfile_hash", carfileInfoTable)
	_, err := c.db.NamedExec(cmd, dInfo)

	return err
}

// CreateOrUpdateCarfileRecordInfo create or update carfile record info
func (c *CarfileDB) CreateOrUpdateCarfileRecordInfo(info *api.CarfileRecordInfo) error {
	cmd := fmt.Sprintf("INSERT INTO %s (carfile_hash, carfile_cid, replica,expired_time) VALUES (:carfile_hash, :carfile_cid, :replica, :expired_time) ON DUPLICATE KEY UPDATE replica=:replica,expired_time=:expired_time", carfileInfoTable)
	_, err := c.db.NamedExec(cmd, info)
	return err
}

// CarfileRecordExisted Carfile record existed
func (c *CarfileDB) CarfileRecordExisted(hash string) (bool, error) {
	var count int
	cmd := fmt.Sprintf("SELECT count(carfile_hash) FROM %s WHERE carfile_hash=?", carfileInfoTable)
	err := c.db.Get(&count, cmd, hash)
	return count > 0, err
}

// LoadCarfileInfo get carfile info with hash
func (c *CarfileDB) LoadCarfileInfo(hash string) (*api.CarfileRecordInfo, error) {
	var info api.CarfileRecordInfo
	cmd := fmt.Sprintf("SELECT * FROM %s WHERE carfile_hash=?", carfileInfoTable)
	err := c.db.Get(&info, cmd, hash)
	return &info, err
}

// LoadCarfileInfos get carfile infos with hashs
func (c *CarfileDB) LoadCarfileInfos(hashs []string) ([]*api.CarfileRecordInfo, error) {
	getCarfilesCmd := fmt.Sprintf(`SELECT * FROM %s WHERE carfile_hash in (?)`, carfileInfoTable)
	carfilesQuery, args, err := sqlx.In(getCarfilesCmd, hashs)
	if err != nil {
		return nil, err
	}
	tx := c.db.MustBegin()

	carfileRecords := make([]*api.CarfileRecordInfo, 0)

	carfilesQuery = c.db.Rebind(carfilesQuery)
	tx.Select(&carfileRecords, carfilesQuery, args...)

	err = tx.Commit()
	if err != nil {
		err = tx.Rollback()
		return nil, err
	}

	return carfileRecords, nil
}

// CarfileRecordInfos get carfile record infos
func (c *CarfileDB) CarfileRecordInfos(page int) (info *api.CarfileRecordsInfo, err error) {
	num := 20

	info = &api.CarfileRecordsInfo{}

	cmd := fmt.Sprintf("SELECT count(carfile_hash) FROM %s ;", carfileInfoTable)
	err = c.db.Get(&info.Cids, cmd)
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

	cmd = fmt.Sprintf("SELECT * FROM %s order by carfile_hash asc LIMIT %d,%d", carfileInfoTable, (num * (page - 1)), num)
	if err = c.db.Select(&info.CarfileRecords, cmd); err != nil {
		return
	}

	return
}

// CandidatesWithHash get candidates with hash
func (c *CarfileDB) CandidatesWithHash(hash string) ([]string, error) {
	var out []string
	query := fmt.Sprintf(`SELECT device_id FROM %s WHERE carfile_hash=? AND status=? AND is_candidate=?`,
		replicaInfoTable)

	if err := c.db.Select(&out, query, hash, api.CacheStatusSucceeded, true); err != nil {
		return nil, err
	}

	return out, nil
}

// CarfileReplicaInfosWithHash get carfile replica infos with hash
func (c *CarfileDB) CarfileReplicaInfosWithHash(hash string, isSuccess bool) ([]*api.ReplicaInfo, error) {
	var out []*api.ReplicaInfo
	if isSuccess {
		query := fmt.Sprintf(`SELECT * FROM %s WHERE carfile_hash=? AND status=?`, replicaInfoTable)

		if err := c.db.Select(&out, query, hash, api.CacheStatusSucceeded); err != nil {
			return nil, err
		}
	} else {
		query := fmt.Sprintf(`SELECT * FROM %s WHERE carfile_hash=? `, replicaInfoTable)

		if err := c.db.Select(&out, query, hash); err != nil {
			return nil, err
		}
	}

	return out, nil
}

// RandomCarfileFromNode Get a random carfile from the node
func (c *CarfileDB) RandomCarfileFromNode(deviceID string) (string, error) {
	query := fmt.Sprintf(`SELECT count(carfile_hash) FROM %s WHERE device_id=? AND status=?`, replicaInfoTable)

	var count int
	if err := c.db.Get(&count, query, deviceID, api.CacheStatusSucceeded); err != nil {
		return "", err
	}

	if count < 1 {
		return "", xerrors.Errorf("node %s no cache", deviceID)
	}

	rand := rand.New(rand.NewSource(time.Now().UnixNano()))
	// rand count
	index := rand.Intn(count)

	var hashs []string
	cmd := fmt.Sprintf("SELECT carfile_hash FROM %s WHERE device_id=? AND status=? LIMIT %d,%d", replicaInfoTable, index, 1)
	if err := c.db.Select(&hashs, cmd, deviceID, api.CacheStatusSucceeded); err != nil {
		return "", err
	}

	if len(hashs) > 0 {
		return hashs[0], nil
	}

	return "", nil
}

// ResetCarfileExpirationTime reset expiration time with carfile record
func (c *CarfileDB) ResetCarfileExpirationTime(carfileHash string, expirationTime time.Time) error {
	tx := c.db.MustBegin()

	cmd := fmt.Sprintf(`UPDATE %s SET expiration_time=? WHERE carfile_hash=?`, carfileInfoTable)
	tx.MustExec(cmd, expirationTime, carfileHash)

	err := tx.Commit()
	if err != nil {
		err = tx.Rollback()
		return err
	}

	return nil
}

func (c *CarfileDB) MinExpirationTime() (time.Time, error) {
	query := fmt.Sprintf(`SELECT MIN(expiration_time) FROM %s`, carfileInfoTable)

	var out time.Time
	if err := c.db.Get(&out, query); err != nil {
		return out, err
	}

	return out, nil
}

func (c *CarfileDB) ExpiredCarfiles() ([]*api.CarfileRecordInfo, error) {
	query := fmt.Sprintf(`SELECT * FROM %s WHERE expiration_time <= NOW()`, carfileInfoTable)

	var out []*api.CarfileRecordInfo
	if err := c.db.Select(&out, query); err != nil {
		return nil, err
	}

	return out, nil
}

// SucceededCachesCount get succeeded caches count
func (c *CarfileDB) SucceededCachesCount() (int, error) {
	query := fmt.Sprintf(`SELECT count(carfile_hash) FROM %s WHERE status=?`, replicaInfoTable)

	var count int
	if err := c.db.Get(&count, query, api.CacheStatusSucceeded); err != nil {
		return 0, err
	}

	return count, nil
}

// LoadReplicaInfo load replica info with id
func (c *CarfileDB) LoadReplicaInfo(id string) (*api.ReplicaInfo, error) {
	var cache api.ReplicaInfo
	query := fmt.Sprintf("SELECT * FROM %s WHERE id=? ", replicaInfoTable)
	if err := c.db.Get(&cache, query, id); err != nil {
		return nil, err
	}

	return &cache, nil
}

// RemoveCarfileRecord remove carfile
func (c *CarfileDB) RemoveCarfileRecord(carfileHash string) error {
	tx := c.db.MustBegin()
	// cache info
	cCmd := fmt.Sprintf(`DELETE FROM %s WHERE carfile_hash=? `, replicaInfoTable)
	tx.MustExec(cCmd, carfileHash)

	// data info
	dCmd := fmt.Sprintf(`DELETE FROM %s WHERE carfile_hash=?`, carfileInfoTable)
	tx.MustExec(dCmd, carfileHash)

	err := tx.Commit()
	if err != nil {
		err = tx.Rollback()
		return err
	}

	return nil
}

// RemoveCarfileReplica remove replica info
func (c *CarfileDB) RemoveCarfileReplica(deviceID, carfileHash string) error {
	tx := c.db.MustBegin()

	// cache info
	cCmd := fmt.Sprintf(`DELETE FROM %s WHERE device_id=? AND carfile_hash=?`, replicaInfoTable)
	tx.MustExec(cCmd, deviceID, carfileHash)

	var count int
	cmd := fmt.Sprintf("SELECT count(*) FROM %s WHERE carfile_hash=? AND status=? AND is_candidate=?", replicaInfoTable)
	err := tx.Get(&count, cmd, carfileHash, api.CacheStatusSucceeded, false)
	if err != nil {
		return err
	}

	err = tx.Commit()
	if err != nil {
		err = tx.Rollback()
		return err
	}

	return nil
}

// LoadCarfileRecordsWithNodes load carfile record hashs with nodes
func (c *CarfileDB) LoadCarfileRecordsWithNodes(deviceIDs []string) (hashs []string, err error) {
	tx := c.db.MustBegin()

	// get carfiles
	getCarfilesCmd := fmt.Sprintf(`select carfile_hash from %s WHERE device_id in (?) GROUP BY carfile_hash`, replicaInfoTable)
	carfilesQuery, args, err := sqlx.In(getCarfilesCmd, deviceIDs)
	if err != nil {
		return
	}

	carfilesQuery = c.db.Rebind(carfilesQuery)
	tx.Select(&hashs, carfilesQuery, args...)

	err = tx.Commit()
	if err != nil {
		err = tx.Rollback()
		return
	}

	return
}

// RemoveReplicaInfoWithNodes remove replica info with nodes
func (c *CarfileDB) RemoveReplicaInfoWithNodes(deviceIDs []string) error {
	tx := c.db.MustBegin()

	// remove cache
	cmd := fmt.Sprintf(`DELETE FROM %s WHERE device_id in (?)`, replicaInfoTable)
	query, args, err := sqlx.In(cmd, deviceIDs)
	if err != nil {
		return err
	}

	query = c.db.Rebind(query)
	tx.MustExec(query, args...)

	err = tx.Commit()
	if err != nil {
		err = tx.Rollback()
		return err
	}

	return nil
}

func (c *CarfileDB) BindNodeAllocateInfo(secret, deviceID string, nodeType api.NodeType) error {
	info := api.NodeAllocateInfo{
		Secret:     secret,
		DeviceID:   deviceID,
		NodeType:   int(nodeType),
		CreateTime: time.Now().Format("2006-01-02 15:04:05"),
	}

	_, err := c.db.NamedExec(`INSERT INTO node_allocate_info (device_id, secret, create_time, node_type)
	VALUES (:device_id, :secret, :create_time, :node_type)`, info)

	return err
}

func (c *CarfileDB) GetNodeAllocateInfo(deviceID, key string, out interface{}) error {
	if key != "" {
		query := fmt.Sprintf(`SELECT %s FROM node_allocate_info WHERE device_id=?`, key)
		if err := c.db.Get(out, query, deviceID); err != nil {
			return err
		}

		return nil
	}

	query := "SELECT * FROM node_allocate_info WHERE device_id=?"
	if err := c.db.Get(out, query, deviceID); err != nil {
		return err
	}

	return nil
}

// download info
func (c *CarfileDB) SetBlockDownloadInfo(info *api.BlockDownloadInfo) error {
	query := fmt.Sprintf(
		`INSERT INTO %s (id, device_id, block_cid, carfile_cid, block_size, speed, reward, status, failed_reason, client_ip, created_time, complete_time) 
				VALUES (:id, :device_id, :block_cid, :carfile_cid, :block_size, :speed, :reward, :status, :failed_reason, :client_ip, :created_time, :complete_time) ON DUPLICATE KEY UPDATE device_id=:device_id, speed=:speed, reward=:reward, status=:status, failed_reason=:failed_reason, complete_time=:complete_time`, blockDownloadInfo)

	_, err := c.db.NamedExec(query, info)
	if err != nil {
		return err
	}

	return nil
}

func (c *CarfileDB) GetBlockDownloadInfoByDeviceID(deviceID string) ([]*api.BlockDownloadInfo, error) {
	query := fmt.Sprintf(`SELECT * FROM %s WHERE device_id = ? and TO_DAYS(created_time) >= TO_DAYS(NOW()) ORDER BY created_time DESC`, blockDownloadInfo)

	var out []*api.BlockDownloadInfo
	if err := c.db.Select(&out, query, deviceID); err != nil {
		return nil, err
	}

	return out, nil
}

func (c *CarfileDB) GetBlockDownloadInfoByID(id string) (*api.BlockDownloadInfo, error) {
	query := fmt.Sprintf(`SELECT * FROM %s WHERE id = ?`, blockDownloadInfo)

	var out []*api.BlockDownloadInfo
	if err := c.db.Select(&out, query, id); err != nil {
		return nil, err
	}

	if len(out) > 0 {
		return out[0], nil
	}
	return nil, nil
}

func (c *CarfileDB) GetNodesByUserDownloadBlockIn(minute int) ([]string, error) {
	starTime := time.Now().Add(time.Duration(minute) * time.Minute * -1)

	query := fmt.Sprintf(`SELECT device_id FROM %s WHERE complete_time > ? group by device_id`, blockDownloadInfo)

	var out []string
	if err := c.db.Select(&out, query, starTime); err != nil {
		return nil, err
	}

	return out, nil
}

func (c *CarfileDB) GetCacheInfosWithNode(deviceID string, index, count int) (info *api.NodeCacheRsp, err error) {
	info = &api.NodeCacheRsp{}

	cmd := fmt.Sprintf("SELECT count(id) FROM %s WHERE device_id=?", replicaInfoTable)
	err = c.db.Get(&info.TotalCount, cmd, deviceID)
	if err != nil {
		return
	}

	cmd = fmt.Sprintf("SELECT carfile_hash,status FROM %s WHERE device_id=? order by id asc LIMIT %d,%d", replicaInfoTable, index, count)
	if err = c.db.Select(&info.Caches, cmd, deviceID); err != nil {
		return
	}

	return
}

func (c *CarfileDB) GetBlockDownloadInfos(deviceID string, startTime time.Time, endTime time.Time, cursor, count int) ([]api.BlockDownloadInfo, int64, error) {
	query := fmt.Sprintf(`SELECT * FROM %s WHERE device_id = ? and created_time between ? and ? limit ?,?`, blockDownloadInfo)

	var total int64
	countSQL := fmt.Sprintf(`SELECT count(*) FROM %s WHERE device_id = ? and created_time between ? and ?`, blockDownloadInfo)
	if err := c.db.Get(&total, countSQL, deviceID, startTime, endTime); err != nil {
		return nil, 0, err
	}

	if count > loadBlockDownloadMaxCount {
		count = loadBlockDownloadMaxCount
	}

	var out []api.BlockDownloadInfo
	if err := c.db.Select(&out, query, deviceID, startTime, endTime, cursor, count); err != nil {
		return nil, 0, err
	}

	return out, total, nil
}

func (c *CarfileDB) GetReplicaInfos(startTime time.Time, endTime time.Time, cursor, count int) (*api.ListCacheInfosRsp, error) {
	var total int64
	countSQL := fmt.Sprintf(`SELECT count(*) FROM %s WHERE end_time between ? and ?`, replicaInfoTable)
	if err := c.db.Get(&total, countSQL, startTime, endTime); err != nil {
		return nil, err
	}

	if count > loadReplicaInfoMaxCount {
		count = loadReplicaInfoMaxCount
	}

	query := fmt.Sprintf(`SELECT * FROM %s WHERE end_time between ? and ? limit ?,?`, replicaInfoTable)

	var out []*api.ReplicaInfo
	if err := c.db.Select(&out, query, startTime, endTime, cursor, count); err != nil {
		return nil, err
	}

	return &api.ListCacheInfosRsp{Datas: out, Total: total}, nil
}

// PushCarfileToWaitList waiting data list
func (c *CarfileDB) PushCarfileToWaitList(info *api.CacheCarfileInfo) error {
	query := fmt.Sprintf(
		`INSERT INTO %s (carfile_hash, carfile_cid, replicas, device_id, expiration_time, server_id) 
				VALUES (:carfile_hash, :carfile_cid, :replicas, :device_id, :expiration_time, :server_id) 
				ON DUPLICATE KEY UPDATE carfile_hash=:carfile_hash, carfile_cid=:carfile_cid, replicas=:replicas, device_id=:device_id, 
				expiration_time=:expiration_time, server_id=:server_id`, waitingCarfileTable)

	_, err := c.db.NamedExec(query, info)
	return err
}

// LoadWaitCarfiles load
func (c *CarfileDB) LoadWaitCarfiles(serverID string) (*api.CacheCarfileInfo, error) {
	sQuery := fmt.Sprintf(`SELECT * FROM %s WHERE server_id=? order by id asc limit ?,?`, waitingCarfileTable)

	info := &api.CacheCarfileInfo{}
	err := c.db.Get(&info, sQuery, serverID, 0, 1)
	if err != nil {
		return nil, err
	}

	return info, nil
}

// RemoveWaitCarfile remove
func (c *CarfileDB) RemoveWaitCarfile(id string) error {
	query := fmt.Sprintf(`DELETE FROM %s WHERE id=?`, waitingCarfileTable)
	_, err := c.db.Exec(query, id)
	return err
}

// GetCachingCarfiles ...
func (c *CarfileDB) GetCachingCarfiles(serverID string) ([]string, error) {
	sQuery := fmt.Sprintf(`SELECT carfile_hash FROM %s WHERE server_id=? GROUP BY carfile_hash`, downloadingTable)

	var out []string
	if err := c.db.Select(&out, sQuery, serverID); err != nil {
		return nil, err
	}
	return out, nil
}

// ReplicaTasksStart ...
func (c *CarfileDB) ReplicaTasksStart(serverID, hash string, deviceIDs []string) error {
	tx := c.db.MustBegin()

	for _, deviceID := range deviceIDs {
		sQuery := fmt.Sprintf(`INSERT INTO %s (carfile_hash, device_id, server_id) VALUES (?, ?, ?)`, downloadingTable)
		tx.MustExec(sQuery, hash, deviceID, serverID)
	}

	err := tx.Commit()
	if err != nil {
		err = tx.Rollback()
	}

	return err
}

// ReplicaTasksEnd ...
func (c *CarfileDB) ReplicaTasksEnd(serverID, hash string, deviceIDs []string) (bool, error) {
	dQuery := fmt.Sprintf("DELETE FROM %s WHERE server_id=? AND carfile_hash=? AND device_id in (?) ", downloadingTable)
	query, args, err := sqlx.In(dQuery, serverID, hash, deviceIDs)
	if err != nil {
		return false, err
	}

	// cache info
	query = c.db.Rebind(query)
	_, err = c.db.Exec(query, args...)
	if err != nil {
		return false, err
	}

	var count int
	sQuery := fmt.Sprintf("SELECT count(*) FROM %s WHERE carfile_hash=? AND server_id=?", downloadingTable)
	err = c.db.Get(&count, sQuery, hash, serverID)
	if err != nil {
		return false, err
	}

	return count == 0, nil
}
