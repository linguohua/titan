package persistent

import (
	"fmt"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/linguohua/titan/api"
	"golang.org/x/xerrors"
)

// TypeSQL Sql
func TypeSQL() string {
	return "Sql"
}

type sqlDB struct {
	cli *sqlx.DB
	url string
}

const errNotFind = "Not Found"

var (
	eventInfoTable    = "event_info_%s"
	carfileInfoTable  = "carfiles_%s"
	cacheInfoTable    = "caches_%s"
	blockDownloadInfo = "block_download_info_%s"
)

// InitSQL init sql
func InitSQL(url string) (DB, error) {
	url = fmt.Sprintf("%s?parseTime=true&loc=Local", url)

	db := &sqlDB{url: url}
	database, err := sqlx.Open("mysql", url)
	if err != nil {
		return nil, err
	}

	if err := database.Ping(); err != nil {
		return nil, err
	}

	db.cli = database

	err = db.setAllNodeOffline()
	return db, err
}

// node info
func (sd sqlDB) SetNodeInfo(deviceID string, info *NodeInfo) error {
	info.DeviceID = deviceID
	info.ServerName = serverName
	// info.CreateTime = time.Now().Format("2006-01-02 15:04:05")

	var count int64
	cmd := "SELECT count(device_id) FROM node WHERE device_id=?"
	err := sd.cli.Get(&count, cmd, deviceID)
	if err != nil {
		return err
	}

	if count == 0 {
		_, err = sd.cli.NamedExec(`INSERT INTO node (device_id, last_time, geo, node_type, is_online, address, server_name,private_key, url)
                VALUES (:device_id, :last_time, :geo, :node_type, :is_online, :address, :server_name,:private_key,:url)`, info)
		return err
	}

	// update
	_, err = sd.cli.NamedExec(`UPDATE node SET last_time=:last_time,geo=:geo,is_online=:is_online,address=:address,server_name=:server_name,url=:url,quitted=:quitted WHERE device_id=:device_id`, info)
	return err
}

func (sd sqlDB) SetNodeOffline(deviceID string, lastTime time.Time) error {
	info := &NodeInfo{
		DeviceID: deviceID,
		LastTime: lastTime,
		IsOnline: false,
	}

	_, err := sd.cli.NamedExec(`UPDATE node SET last_time=:last_time,is_online=:is_online WHERE device_id=:device_id`, info)

	return err
}

func (sd sqlDB) GetNodeAuthInfo(deviceID string) (*api.DownloadServerAccessAuth, error) {
	info := &NodeInfo{DeviceID: deviceID}

	rows, err := sd.cli.NamedQuery(`SELECT private_key,url FROM node WHERE device_id=:device_id`, info)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	if rows.Next() {
		err = rows.StructScan(info)
		if err != nil {
			return nil, err
		}
	} else {
		return nil, xerrors.New(errNotFind)
	}

	return &api.DownloadServerAccessAuth{URL: info.URL, PrivateKey: info.PrivateKey, DeviceID: deviceID}, err
}

func (sd sqlDB) GetOfflineNodes() ([]*NodeInfo, error) {
	list := make([]*NodeInfo, 0)

	cmd := "SELECT device_id,last_time FROM node WHERE quitted=? AND is_online=? AND server_name=?"
	if err := sd.cli.Select(&list, cmd, false, false, serverName); err != nil {
		return nil, err
	}

	return list, nil
}

func (sd sqlDB) SetNodesQuit(deviceIDs []string) error {
	tx := sd.cli.MustBegin()

	for _, deviceID := range deviceIDs {
		dCmd := `UPDATE node SET quitted=? WHERE device_id=?`
		tx.MustExec(dCmd, true, deviceID)
	}

	err := tx.Commit()
	if err != nil {
		err = tx.Rollback()
		return err
	}

	return nil
}

// Validate Result
func (sd sqlDB) InsertValidateResultInfo(info *ValidateResult) error {
	query := fmt.Sprintf("INSERT INTO%s", " validate_result (round_id, device_id, validator_id, status, start_time, server_name, msg) VALUES (:round_id, :device_id, :validator_id, :status, :start_time, :server_name, :msg)")
	_, err := sd.cli.NamedExec(query, info)
	return err
}

// Validate Result
func (sd sqlDB) InsertValidateResultInfos(infos []*ValidateResult) error {
	tx := sd.cli.MustBegin()
	for _, info := range infos {
		query := fmt.Sprintf("INSERT INTO%s", " validate_result (round_id, device_id, validator_id, status, start_time, server_name) VALUES (?, ?, ?, ?, ?, ?)")
		tx.MustExec(query, info.RoundID, info.DeviceID, info.ValidatorID, info.Status, info.StartTime, serverName)
	}

	err := tx.Commit()
	if err != nil {
		err = tx.Rollback()
		return err
	}

	return nil
}

func (sd sqlDB) UpdateFailValidateResultInfo(info *ValidateResult) error {
	query := fmt.Sprintf("UPDATE%s", " validate_result SET msg=:msg, status=:status, end_time=:end_time WHERE round_id=:round_id AND device_id=:device_id")
	_, err := sd.cli.NamedExec(query, info)
	return err
}

func (sd sqlDB) SetTimeoutToValidateInfos(info *ValidateResult, deviceIDs []string) error {
	tx := sd.cli.MustBegin()

	updateCachesCmd := `UPDATE validate_result SET status=?,end_time=? WHERE round_id=? AND device_id in (?)`
	query, args, err := sqlx.In(updateCachesCmd, info.Status, info.EndTime, info.RoundID, deviceIDs)
	if err != nil {
		return err
	}

	// cache info
	query = sd.cli.Rebind(query)
	tx.MustExec(query, args...)

	err = tx.Commit()
	if err != nil {
		err = tx.Rollback()
		return err
	}

	return nil
}

func (sd sqlDB) UpdateSuccessValidateResultInfo(info *ValidateResult) error {
	query := fmt.Sprintf("UPDATE%s", " validate_result SET block_number=:block_number,status=:status, duration=:duration, bandwidth=:bandwidth, end_time=:end_time WHERE round_id=:round_id AND device_id=:device_id")
	_, err := sd.cli.NamedExec(query, info)
	return err
}

func (sd sqlDB) SummaryValidateMessage(startTime, endTime time.Time, pageNumber, pageSize int) (*api.SummeryValidateResult, error) {
	res := new(api.SummeryValidateResult)
	var infos []api.ValidateResultInfo
	query := fmt.Sprintf("SELECT%s%s%v%s%v%s%d,%d;",
		" `device_id`,`validator_id`,`block_number`,`status`,`start_time` AS `validate_time`, `duration`, (duration/1e3 * bandwidth) AS `upload_traffic` FROM validate_result",
		" WHERE start_time >= '",
		startTime,
		"' AND end_time <= '",
		endTime,
		"' LIMIT ",
		(pageNumber-1)*pageSize,
		pageSize)
	err := sd.cli.Select(&infos, query)
	if err != nil {
		return nil, err
	}

	res.ValidateResultInfos = infos

	countQuery := fmt.Sprintf("SELECT%s%s%v%s%v%s;",
		" COUNT(*) FROM validate_result",
		" WHERE start_time >= '",
		startTime,
		"' AND end_time <= '",
		endTime,
		"'",
	)

	var count int
	err = sd.cli.Get(&count, countQuery)
	if err != nil {
		return nil, err
	}

	res.Total = count

	return res, nil
}

// cache data info
func (sd sqlDB) CreateCacheTaskInfo(cInfo *api.CacheTaskInfo) error {
	area := sd.ReplaceArea()
	cTableName := fmt.Sprintf(cacheInfoTable, area)

	cmd := fmt.Sprintf("INSERT INTO %s (carfile_hash, device_id, status, root_cache) VALUES (:carfile_hash, :device_id, :status, :root_cache)", cTableName)
	_, err := sd.cli.NamedExec(cmd, cInfo)
	return err
}

func (sd sqlDB) UpdateCacheTaskStatus(cInfo *api.CacheTaskInfo) error {
	area := sd.ReplaceArea()
	cTableName := fmt.Sprintf(cacheInfoTable, area)

	cmd := fmt.Sprintf("UPDATE %s SET status=:status WHERE carfile_hash=:carfile_hash AND device_id=:device_id", cTableName)
	_, err := sd.cli.NamedExec(cmd, cInfo)

	return err
}

func (sd sqlDB) UpdateCacheTaskInfo(cInfo *api.CacheTaskInfo) error {
	area := sd.ReplaceArea()
	cTableName := fmt.Sprintf(cacheInfoTable, area)

	cmd := fmt.Sprintf("UPDATE %s SET done_size=:done_size,done_blocks=:done_blocks,reliability=:reliability,status=:status,end_time=:end_time,execute_count=:execute_count WHERE carfile_hash=:carfile_hash AND device_id=:device_id", cTableName)
	_, err := sd.cli.NamedExec(cmd, cInfo)

	return err
}

// func (sd sqlDB) SaveCacheResults(dInfo *api.CarfileRecordInfo, cInfo *api.CacheTaskInfo) error {
// 	area := sd.ReplaceArea()
// 	dTableName := fmt.Sprintf(carfileInfoTable, area)
// 	cTableName := fmt.Sprintf(cacheInfoTable, area)

// 	tx := sd.cli.MustBegin()

// 	// data info
// 	dCmd := fmt.Sprintf("UPDATE %s SET total_size=?,reliability=?,total_blocks=?,end_time=NOW() WHERE carfile_hash=?", dTableName)
// 	tx.MustExec(dCmd, dInfo.TotalSize, dInfo.Reliability, dInfo.TotalBlocks, dInfo.CarfileHash)

// 	// cache info
// 	cCmd := fmt.Sprintf(`UPDATE %s SET done_size=?,done_blocks=?,reliability=?,status=?,end_time=NOW(),execute_count=? WHERE device_id=? AND carfile_hash=?`, cTableName)
// 	tx.MustExec(cCmd, cInfo.DoneSize, cInfo.DoneBlocks, cInfo.Reliability, cInfo.Status, cInfo.ExecuteCount, cInfo.DeviceID, cInfo.CarfileHash)

// 	err := tx.Commit()
// 	if err != nil {
// 		err = tx.Rollback()
// 	}

// 	return err
// }

func (sd sqlDB) UpdateCarfileRecordCachesInfo(dInfo *api.CarfileRecordInfo) error {
	area := sd.ReplaceArea()
	dTableName := fmt.Sprintf(carfileInfoTable, area)

	// update
	cmd := fmt.Sprintf("UPDATE %s SET total_size=:total_size,total_blocks=:total_blocks,reliability=:reliability,end_time=NOW() WHERE carfile_hash=:carfile_hash", dTableName)
	_, err := sd.cli.NamedExec(cmd, dInfo)

	return err
}

func (sd sqlDB) UpdateCarfileRecordBasisInfo(info *api.CarfileRecordInfo) error {
	area := sd.ReplaceArea()

	tableName := fmt.Sprintf(carfileInfoTable, area)

	oldInfo, err := sd.GetCarfileInfo(info.CarfileHash)
	if err != nil && !sd.IsNilErr(err) {
		return err
	}

	if oldInfo == nil {
		cmd := fmt.Sprintf("INSERT INTO %s (carfile_hash, carfile_cid, need_reliability,expired_time) VALUES (:carfile_hash, :carfile_cid, :need_reliability, :expired_time)", tableName)
		_, err = sd.cli.NamedExec(cmd, info)
		return err
	}

	// update
	cmd := fmt.Sprintf("UPDATE %s SET expired_time=:expired_time,need_reliability=:need_reliability WHERE carfile_hash=:carfile_hash", tableName)
	_, err = sd.cli.NamedExec(cmd, info)

	return err
}

func (sd sqlDB) GetCarfileInfo(hash string) (*api.CarfileRecordInfo, error) {
	area := sd.ReplaceArea()

	info := &api.CarfileRecordInfo{CarfileHash: hash}
	cmd := fmt.Sprintf("SELECT * FROM %s WHERE carfile_hash=:carfile_hash", fmt.Sprintf(carfileInfoTable, area))

	rows, err := sd.cli.NamedQuery(cmd, info)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	if rows.Next() {
		err = rows.StructScan(info)
		if err != nil {
			return nil, err
		}
	} else {
		return nil, xerrors.New(errNotFind)
	}

	return info, err
}

func (sd sqlDB) GetCarfileCidWithPage(page int) (count int, totalPage int, list []*api.CarfileRecordInfo, err error) {
	area := sd.ReplaceArea()
	num := 20

	cmd := fmt.Sprintf("SELECT count(carfile_cid) FROM %s ;", fmt.Sprintf(carfileInfoTable, area))
	err = sd.cli.Get(&count, cmd)
	if err != nil {
		return
	}

	totalPage = count / num
	if count%num > 0 {
		totalPage++
	}

	if totalPage == 0 {
		return
	}

	if page > totalPage {
		page = totalPage
	}

	cmd = fmt.Sprintf("SELECT * FROM %s LIMIT %d,%d", fmt.Sprintf(carfileInfoTable, area), (num * (page - 1)), num)
	if err = sd.cli.Select(&list, cmd); err != nil {
		return
	}

	return
}

// func (sd sqlDB) GetCandidateWithHash(hash string) ([]*api.CacheTaskInfo, error) {
// 	area := sd.ReplaceArea()

// 	var out []*api.CacheTaskInfo
// 	query := fmt.Sprintf(`SELECT * FROM %s WHERE carfile_hash=? AND status=? AND root_cache=?`,
// 		fmt.Sprintf(cacheInfoTable, area))

// 	if err := sd.cli.Select(&out, query, hash, api.CacheStatusSuccess, true); err != nil {
// 		return nil, err
// 	}

// 	return out, nil
// }

func (sd sqlDB) GetCachesWithCandidate(hash string) ([]string, error) {
	area := sd.ReplaceArea()

	var out []string
	query := fmt.Sprintf(`SELECT device_id FROM %s WHERE carfile_hash=? AND status=? AND root_cache=?`,
		fmt.Sprintf(cacheInfoTable, area))

	if err := sd.cli.Select(&out, query, hash, api.CacheStatusSuccess, true); err != nil {
		return nil, err
	}

	return out, nil
}

func (sd sqlDB) GetCaches(hash string, isSuccess bool) ([]*api.CacheTaskInfo, error) {
	area := sd.ReplaceArea()

	var out []*api.CacheTaskInfo
	if isSuccess {
		query := fmt.Sprintf(`SELECT * FROM %s WHERE carfile_hash=? AND status=?`,
			fmt.Sprintf(cacheInfoTable, area))

		if err := sd.cli.Select(&out, query, hash, api.CacheStatusSuccess); err != nil {
			return nil, err
		}
	} else {
		query := fmt.Sprintf(`SELECT * FROM %s WHERE carfile_hash=? `,
			fmt.Sprintf(cacheInfoTable, area))

		if err := sd.cli.Select(&out, query, hash); err != nil {
			return nil, err
		}
	}

	return out, nil
}

func (sd sqlDB) GetRandCarfileWithNode(deviceID string) (string, error) {
	query := fmt.Sprintf(`SELECT count(carfile_hash) FROM %s WHERE device_id=? AND status=?`,
		fmt.Sprintf(cacheInfoTable, sd.ReplaceArea()))

	var count int
	if err := sd.cli.Get(&count, query, deviceID, api.CacheStatusSuccess); err != nil {
		return "", err
	}

	if count < 1 {
		return "", xerrors.Errorf("node %s no cache", deviceID)
	}

	// rand count
	index := myRand.Intn(count)

	var hashs []string
	cmd := fmt.Sprintf("SELECT carfile_hash FROM %s WHERE device_id=? AND status=? LIMIT %d,%d", fmt.Sprintf(cacheInfoTable, sd.ReplaceArea()), index, 1)
	if err := sd.cli.Select(&hashs, cmd, deviceID, api.CacheStatusSuccess); err != nil {
		return "", err
	}

	// fmt.Println("hashs :", hashs)

	if len(hashs) > 0 {
		return hashs[0], nil
	}

	return "", nil
}

func (sd sqlDB) ExtendExpiredTimeWhitCarfile(carfileHash string, hour int) error {
	tx := sd.cli.MustBegin()

	cmd := fmt.Sprintf(`UPDATE %s SET expired_time=DATE_ADD(expired_time,interval ? HOUR) WHERE carfile_hash=?`, fmt.Sprintf(carfileInfoTable, sd.ReplaceArea()))
	tx.MustExec(cmd, hour, carfileHash)

	err := tx.Commit()
	if err != nil {
		err = tx.Rollback()
		return err
	}

	return nil
}

func (sd sqlDB) ChangeExpiredTimeWhitCarfile(carfileHash string, expiredTime time.Time) error {
	tx := sd.cli.MustBegin()

	cmd := fmt.Sprintf(`UPDATE %s SET expired_time=? WHERE carfile_hash=?`, fmt.Sprintf(carfileInfoTable, sd.ReplaceArea()))
	tx.MustExec(cmd, expiredTime, carfileHash)

	err := tx.Commit()
	if err != nil {
		err = tx.Rollback()
		return err
	}

	return nil
}

// func (sd sqlDB) GetCarfileCount() (int, error) {
// 	query := fmt.Sprintf(`SELECT count(carfile_hash) FROM %s`,
// 		fmt.Sprintf(carfileInfoTable, sd.ReplaceArea()))

// 	var out int
// 	if err := sd.cli.Get(&out, query); err != nil {
// 		return out, err
// 	}

// 	return out, nil
// }

func (sd sqlDB) GetMinExpiredTime() (time.Time, error) {
	query := fmt.Sprintf(`SELECT MIN(expired_time) FROM %s`,
		fmt.Sprintf(carfileInfoTable, sd.ReplaceArea()))

	var out time.Time
	if err := sd.cli.Get(&out, query); err != nil {
		return out, err
	}

	return out, nil
}

func (sd sqlDB) GetExpiredCarfiles() ([]*api.CarfileRecordInfo, error) {
	query := fmt.Sprintf(`SELECT * FROM %s WHERE expired_time <= NOW()`,
		fmt.Sprintf(carfileInfoTable, sd.ReplaceArea()))

	var out []*api.CarfileRecordInfo
	if err := sd.cli.Select(&out, query); err != nil {
		return nil, err
	}

	return out, nil
}

func (sd sqlDB) GetSuccessCachesCount() (int, error) {
	query := fmt.Sprintf(`SELECT count(carfile_hash) FROM %s WHERE status=?`,
		fmt.Sprintf(cacheInfoTable, sd.ReplaceArea()))

	// var out []*api.CacheTaskInfo
	// if err := sd.cli.Select(&out, query, api.CacheStatusSuccess); err != nil {
	// 	return nil, err
	// }

	var count int
	if err := sd.cli.Get(&count, query, api.CacheStatusSuccess); err != nil {
		return 0, err
	}

	return count, nil
}

func (sd sqlDB) GetCacheInfo(carfileHash, deviceID string) (*api.CacheTaskInfo, error) {
	area := sd.ReplaceArea()

	var cache api.CacheTaskInfo
	query := fmt.Sprintf("SELECT * FROM %s WHERE device_id=? AND carfile_hash=?", fmt.Sprintf(cacheInfoTable, area))
	if err := sd.cli.Get(&cache, query, deviceID, carfileHash); err != nil {
		return nil, err
	}

	return &cache, nil
}

func (sd sqlDB) RemoveCarfileRecord(carfileHash string) error {
	area := sd.ReplaceArea()
	cTableName := fmt.Sprintf(cacheInfoTable, area)
	dTableName := fmt.Sprintf(carfileInfoTable, area)

	tx := sd.cli.MustBegin()
	// cache info
	cCmd := fmt.Sprintf(`DELETE FROM %s WHERE carfile_hash=? `, cTableName)
	tx.MustExec(cCmd, carfileHash)

	// data info
	dCmd := fmt.Sprintf(`DELETE FROM %s WHERE carfile_hash=?`, dTableName)
	tx.MustExec(dCmd, carfileHash)

	err := tx.Commit()
	if err != nil {
		err = tx.Rollback()
		return err
	}

	return nil
}

// remove cache info and update data info
func (sd sqlDB) RemoveCacheTask(deviceID, carfileHash string) error {
	area := sd.ReplaceArea()
	cTableName := fmt.Sprintf(cacheInfoTable, area)
	dTableName := fmt.Sprintf(carfileInfoTable, area)

	tx := sd.cli.MustBegin()

	// cache info
	cCmd := fmt.Sprintf(`DELETE FROM %s WHERE device_id=? `, cTableName)
	tx.MustExec(cCmd, deviceID)

	var reliability int
	cmd := fmt.Sprintf("SELECT sum(reliability) FROM %s WHERE carfile_hash=? ", cTableName)
	err := tx.Get(&reliability, cmd, carfileHash)
	if err != nil {
		return err
	}

	// data info
	if reliability > 0 {
		dCmd := fmt.Sprintf("UPDATE %s SET reliability=? WHERE carfile_hash=?", dTableName)
		tx.MustExec(dCmd, reliability, carfileHash)
	} else {
		dCmd := fmt.Sprintf(`DELETE FROM %s WHERE carfile_hash=?`, dTableName)
		tx.MustExec(dCmd, carfileHash)
	}

	err = tx.Commit()
	if err != nil {
		err = tx.Rollback()
		return err
	}

	return nil
}

func (sd sqlDB) UpdateCacheInfoOfQuitNode(deviceIDs []string) (carfileRecords []*api.CarfileRecordInfo, err error) {
	area := sd.ReplaceArea()
	cTableName := fmt.Sprintf(cacheInfoTable, area)
	dTableName := fmt.Sprintf(carfileInfoTable, area)
	// select * from (
	// 	select carfile_hash from caches_cn_gd_shenzhen WHERE device_id in ('c_9e7c920e5f5a11edbdbb902e1671f843') GROUP BY carfile_hash )as a
	// 	LEFT JOIN carfiles_cn_gd_shenzhen as b on a.carfile_hash = b.carfile_hash

	tx := sd.cli.MustBegin()

	// get carfiles
	getCarfilesCmd := fmt.Sprintf(`select * from (
		select carfile_hash from %s WHERE device_id in (?) GROUP BY carfile_hash )as a 
		LEFT JOIN %s as b on a.carfile_hash = b.carfile_hash`, cTableName, dTableName)
	carfilesQuery, args, err := sqlx.In(getCarfilesCmd, deviceIDs)
	if err != nil {
		return
	}

	// var carfiles []*api.CarfileRecordInfo
	carfilesQuery = sd.cli.Rebind(carfilesQuery)
	tx.Select(&carfileRecords, carfilesQuery, args...)

	// remove cache
	removeCachesCmd := fmt.Sprintf(`DELETE FROM %s WHERE device_id in (?)`, cTableName)
	removeCacheQuery, args, err := sqlx.In(removeCachesCmd, deviceIDs)
	if err != nil {
		return
	}

	removeCacheQuery = sd.cli.Rebind(removeCacheQuery)
	tx.MustExec(removeCacheQuery, args...)

	// update carfiles record
	for _, carfileRecord := range carfileRecords {
		var reliability int64
		cmd := fmt.Sprintf("SELECT sum(reliability) FROM %s WHERE carfile_hash=? ", cTableName)
		err := tx.Get(&reliability, cmd, carfileRecord.CarfileHash)
		if err != nil {
			continue
		}

		cmdD := fmt.Sprintf(`UPDATE %s SET reliability=? WHERE carfile_hash=?`, dTableName)
		tx.MustExec(cmdD, reliability, carfileRecord.CarfileHash)
	}

	err = tx.Commit()
	if err != nil {
		err = tx.Rollback()
		return
	}

	return
}

// temporary node register
func (sd sqlDB) BindRegisterInfo(secret, deviceID string, nodeType api.NodeType) error {
	info := api.NodeRegisterInfo{
		Secret:     secret,
		DeviceID:   deviceID,
		NodeType:   int(nodeType),
		CreateTime: time.Now().Format("2006-01-02 15:04:05"),
	}

	_, err := sd.cli.NamedExec(`INSERT INTO register (device_id, secret, create_time, node_type)
	VALUES (:device_id, :secret, :create_time, :node_type)`, info)

	return err
}

func (sd sqlDB) GetRegisterInfo(deviceID, key string, out interface{}) error {
	if key != "" {
		query := fmt.Sprintf(`SELECT %s FROM register WHERE device_id=?`, key)
		// query := "SELECT * FROM register WHERE device_id=?"
		if err := sd.cli.Get(out, query, deviceID); err != nil {
			return err
		}

		return nil
	}

	query := "SELECT * FROM register WHERE device_id=?"
	if err := sd.cli.Get(out, query, deviceID); err != nil {
		return err
	}

	return nil
}

// download info
func (sd sqlDB) SetBlockDownloadInfo(info *api.BlockDownloadInfo) error {
	query := fmt.Sprintf(
		`INSERT INTO %s (id, device_id, block_cid, carfile_cid, block_size, speed, reward, status, failed_reason, client_ip, created_time, complete_time) 
				VALUES (:id, :device_id, :block_cid, :carfile_cid, :block_size, :speed, :reward, :status, :failed_reason, :client_ip, :created_time, :complete_time) ON DUPLICATE KEY UPDATE device_id=:device_id, speed=:speed, reward=:reward, status=:status, failed_reason=:failed_reason, complete_time=:complete_time`, fmt.Sprintf(blockDownloadInfo, sd.ReplaceArea()))

	_, err := sd.cli.NamedExec(query, info)
	if err != nil {
		return err
	}

	return nil
}

func (sd sqlDB) GetBlockDownloadInfoByDeviceID(deviceID string) ([]*api.BlockDownloadInfo, error) {
	query := fmt.Sprintf(`SELECT * FROM %s WHERE device_id = ? and TO_DAYS(created_time) >= TO_DAYS(NOW()) ORDER BY created_time DESC`,
		fmt.Sprintf(blockDownloadInfo, sd.ReplaceArea()))

	var out []*api.BlockDownloadInfo
	if err := sd.cli.Select(&out, query, deviceID); err != nil {
		return nil, err
	}

	return out, nil
}

func (sd sqlDB) GetBlockDownloadInfoByID(id string) (*api.BlockDownloadInfo, error) {
	query := fmt.Sprintf(`SELECT * FROM %s WHERE id = ?`,
		fmt.Sprintf(blockDownloadInfo, sd.ReplaceArea()))

	var out []*api.BlockDownloadInfo
	if err := sd.cli.Select(&out, query, id); err != nil {
		return nil, err
	}

	if len(out) > 0 {
		return out[0], nil
	}
	return nil, nil
}

func (sd sqlDB) GetNodesByUserDownloadBlockIn(minute int) ([]string, error) {
	starTime := time.Now().Add(time.Duration(minute) * time.Minute * -1)

	query := fmt.Sprintf(`SELECT device_id FROM %s WHERE complete_time > ? group by device_id`, fmt.Sprintf(blockDownloadInfo, sd.ReplaceArea()))

	var out []string
	if err := sd.cli.Select(&out, query, starTime); err != nil {
		return nil, err
	}

	return out, nil
}

// // cache event info
// func (sd sqlDB) SetEventInfo(info *api.EventInfo) error {
// 	area := sd.ReplaceArea()

// 	tableName := fmt.Sprintf(eventInfoTable, area)

// 	cmd := fmt.Sprintf("INSERT INTO %s (cid, device_id, user, event, msg) VALUES (:cid, :device_id, :user, :event, :msg)", tableName)
// 	_, err := sd.cli.NamedExec(cmd, info)
// 	return err
// }

// func (sd sqlDB) GetEventInfos(page int) (count int, totalPage int, out []*api.EventInfo, err error) {
// 	area := "cn_gd_shenzhen"
// 	num := 20

// 	cmd := fmt.Sprintf("SELECT count(cid) FROM %s ;", fmt.Sprintf(eventInfoTable, area))
// 	err = sd.cli.Get(&count, cmd)
// 	if err != nil {
// 		return
// 	}

// 	totalPage = count / num
// 	if count%num > 0 {
// 		totalPage++
// 	}

// 	if totalPage == 0 {
// 		return
// 	}

// 	if page > totalPage {
// 		page = totalPage
// 	}

// 	cmd = fmt.Sprintf("SELECT * FROM %s LIMIT %d,%d", fmt.Sprintf(eventInfoTable, area), (num * (page - 1)), num)
// 	if err = sd.cli.Select(&out, cmd); err != nil {
// 		return
// 	}

// 	return
// }

// IsNilErr Is NilErr
func (sd sqlDB) IsNilErr(err error) bool {
	return err.Error() == errNotFind
}

func (sd sqlDB) setAllNodeOffline() error {
	info := &NodeInfo{IsOnline: false, ServerName: serverName}
	_, err := sd.cli.NamedExec(`UPDATE node SET is_online=:is_online WHERE server_name=:server_name`, info)

	return err
}

func (sd sqlDB) ReplaceArea() string {
	str := strings.ToLower(serverArea)
	str = strings.Replace(str, "-", "_", -1)

	return str
}
