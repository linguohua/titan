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
	dataInfoTable     = "data_info_%s"
	cacheInfoTable    = "cache_info_%s"
	blockDownloadInfo = "block_download_info_%s"
	// messageInfoTable  = "message_info_%s"
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

func (sd sqlDB) UpdateFailValidateResultInfo(info *ValidateResult) error {
	query := fmt.Sprintf("UPDATE%s", " validate_result SET msg=:msg, status=:status, end_time=:end_time WHERE round_id=:round_id AND device_id=:device_id")
	_, err := sd.cli.NamedExec(query, info)
	return err
}

func (sd sqlDB) UpdateSuccessValidateResultInfo(info *ValidateResult) error {
	query := fmt.Sprintf("UPDATE%s", " validate_result SET block_number=:block_number, msg=:msg, status=:status, duration=:duration, bandwidth=:bandwidth, end_time=:end_time WHERE round_id=:round_id AND device_id=:device_id")
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
func (sd sqlDB) CreateCache(cInfo *api.CacheInfo) error {
	area := sd.ReplaceArea()
	cTableName := fmt.Sprintf(cacheInfoTable, area)

	tx := sd.cli.MustBegin()

	// cache info
	cCmd := fmt.Sprintf("INSERT INTO %s (carfile_hash, device_id, status, expired_time, root_cache) VALUES (?, ?, ?, ?, ?)", cTableName)
	tx.MustExec(cCmd, cInfo.CarfileHash, cInfo.DeviceID, cInfo.Status, cInfo.ExpiredTime, cInfo.RootCache)

	err := tx.Commit()
	if err != nil {
		err = tx.Rollback()
		return err
	}

	return nil
}

func (sd sqlDB) SaveCacheEndResults(dInfo *api.DataInfo, cInfo *api.CacheInfo) error {
	area := sd.ReplaceArea()
	dTableName := fmt.Sprintf(dataInfoTable, area)
	cTableName := fmt.Sprintf(cacheInfoTable, area)
	tx := sd.cli.MustBegin()

	// data info
	dCmd := fmt.Sprintf("UPDATE %s SET total_size=?,reliability=?,total_blocks=?,end_time=NOW() WHERE carfile_hash=?", dTableName)
	tx.MustExec(dCmd, dInfo.TotalSize, dInfo.Reliability, dInfo.TotalBlocks, dInfo.CarfileHash)

	// cache info
	cCmd := fmt.Sprintf(`UPDATE %s SET done_size=?,done_blocks=?,reliability=?,status=?,end_time=NOW() WHERE device_id=? `, cTableName)
	tx.MustExec(cCmd, cInfo.DoneSize, cInfo.DoneBlocks, cInfo.Reliability, cInfo.Status, cInfo.DeviceID)

	err := tx.Commit()
	if err != nil {
		err = tx.Rollback()
	}

	return err
}

func (sd sqlDB) SaveCacheingResults(dInfo *api.DataInfo, cInfo *api.CacheInfo) error {
	area := sd.ReplaceArea()
	dTableName := fmt.Sprintf(dataInfoTable, area)
	cTableName := fmt.Sprintf(cacheInfoTable, area)

	tx := sd.cli.MustBegin()

	if dInfo != nil {
		// data info
		dCmd := fmt.Sprintf("UPDATE %s SET total_size=?,reliability=?,total_blocks=? WHERE carfile_hash=?", dTableName)
		tx.MustExec(dCmd, dInfo.TotalSize, dInfo.Reliability, dInfo.TotalBlocks, dInfo.CarfileHash)
	}

	if cInfo != nil {
		// cache info
		cCmd := fmt.Sprintf(`UPDATE %s SET done_size=?,done_blocks=?,cache_count=?,status=? WHERE device_id=? `, cTableName)
		tx.MustExec(cCmd, cInfo.DoneSize, cInfo.DoneBlocks, cInfo.CacheCount, cInfo.Status, cInfo.DeviceID)
	}

	err := tx.Commit()
	if err != nil {
		err = tx.Rollback()
	}

	return err
}

func (sd sqlDB) SetDataInfo(info *api.DataInfo) error {
	area := sd.ReplaceArea()

	tableName := fmt.Sprintf(dataInfoTable, area)

	oldInfo, err := sd.GetDataInfo(info.CarfileHash)
	if err != nil && !sd.IsNilErr(err) {
		return err
	}

	if oldInfo == nil {
		cmd := fmt.Sprintf("INSERT INTO %s (carfile_hash, carfile_cid, status, need_reliability, total_blocks, expired_time) VALUES (:carfile_hash, :carfile_cid, :status, :need_reliability, :total_blocks, :expired_time)", tableName)
		_, err = sd.cli.NamedExec(cmd, info)
		return err
	}

	// update
	cmd := fmt.Sprintf("UPDATE %s SET expired_time=:expired_time,status=:status,total_size=:total_size,reliability=:reliability,cache_count=:cache_count,total_blocks=:total_blocks,need_reliability=:need_reliability WHERE carfile_hash=:carfile_hash", tableName)
	_, err = sd.cli.NamedExec(cmd, info)

	return err
}

func (sd sqlDB) GetDataInfo(hash string) (*api.DataInfo, error) {
	area := sd.ReplaceArea()

	info := &api.DataInfo{CarfileHash: hash}
	cmd := fmt.Sprintf("SELECT * FROM %s WHERE carfile_hash=:carfile_hash", fmt.Sprintf(dataInfoTable, area))

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

func (sd sqlDB) GetDataCidWithPage(page int) (count int, totalPage int, list []*api.DataInfo, err error) {
	area := sd.ReplaceArea()
	num := 20

	cmd := fmt.Sprintf("SELECT count(carfile_cid) FROM %s ;", fmt.Sprintf(dataInfoTable, area))
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

	cmd = fmt.Sprintf("SELECT * FROM %s LIMIT %d,%d", fmt.Sprintf(dataInfoTable, area), (num * (page - 1)), num)
	if err = sd.cli.Select(&list, cmd); err != nil {
		return
	}

	return
}

func (sd sqlDB) GetCachesWithData(hash string, isSuccess bool) ([]*api.CacheInfo, error) {
	area := sd.ReplaceArea()

	var out []*api.CacheInfo
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

func (sd sqlDB) ExtendExpiredTimeWhitCaches(carfileHash, cacheID string, hour int) error {
	tx := sd.cli.MustBegin()

	if cacheID == "" {
		cmd := fmt.Sprintf(`UPDATE %s SET expired_time=DATE_ADD(expired_time,interval ? HOUR) WHERE carfile_hash=?`, fmt.Sprintf(cacheInfoTable, sd.ReplaceArea()))
		tx.MustExec(cmd, hour, carfileHash)
	} else {
		cmd := fmt.Sprintf(`UPDATE %s SET expired_time=DATE_ADD(expired_time,interval ? HOUR) WHERE carfile_hash=? AND device_id=?`, fmt.Sprintf(cacheInfoTable, sd.ReplaceArea()))
		tx.MustExec(cmd, hour, carfileHash, cacheID)
	}

	err := tx.Commit()
	if err != nil {
		err = tx.Rollback()
		return err
	}

	return nil
}

func (sd sqlDB) ChangeExpiredTimeWhitCaches(carfileHash, cacheID string, expiredTime time.Time) error {
	tx := sd.cli.MustBegin()

	if cacheID == "" {
		// cmd := fmt.Sprintf(`UPDATE %s SET expired_time=DATE_ADD(expired_time,interval ? HOUR) WHERE carfile_hash=?`, fmt.Sprintf(cacheInfoTable, sd.ReplaceArea()))
		cmd := fmt.Sprintf(`UPDATE %s SET expired_time=? WHERE carfile_hash=?`, fmt.Sprintf(cacheInfoTable, sd.ReplaceArea()))
		tx.MustExec(cmd, expiredTime, carfileHash)
	} else {
		// cmd := fmt.Sprintf(`UPDATE %s SET expired_time=DATE_ADD(expired_time,interval ? HOUR) WHERE carfile_hash=? AND device_id=?`, fmt.Sprintf(cacheInfoTable, sd.ReplaceArea()))
		cmd := fmt.Sprintf(`UPDATE %s SET expired_time=? WHERE carfile_hash=? AND device_id=?`, fmt.Sprintf(cacheInfoTable, sd.ReplaceArea()))
		tx.MustExec(cmd, expiredTime, carfileHash, cacheID)
	}

	// cmdData := fmt.Sprintf(`UPDATE %s SET expired_time=? WHERE carfile_hash=?`, fmt.Sprintf(dataInfoTable, sd.ReplaceArea()))
	// tx.MustExec(cmdData, time, carfileHash)

	err := tx.Commit()
	if err != nil {
		err = tx.Rollback()
		return err
	}

	return nil
}

func (sd sqlDB) GetMinExpiredTimeWithCaches() (time.Time, error) {
	query := fmt.Sprintf(`SELECT MIN(expired_time) FROM %s`,
		fmt.Sprintf(cacheInfoTable, sd.ReplaceArea()))

	var out time.Time
	if err := sd.cli.Get(&out, query); err != nil {
		return out, err
	}

	return out, nil
}

func (sd sqlDB) GetExpiredCaches() ([]*api.CacheInfo, error) {
	query := fmt.Sprintf(`SELECT carfile_hash,device_id FROM %s WHERE expired_time <= NOW()`,
		fmt.Sprintf(cacheInfoTable, sd.ReplaceArea()))

	var out []*api.CacheInfo
	if err := sd.cli.Select(&out, query); err != nil {
		return nil, err
	}

	return out, nil
}

func (sd sqlDB) GetSuccessCaches() ([]*api.CacheInfo, error) {
	query := fmt.Sprintf(`SELECT * FROM %s WHERE status=?`,
		fmt.Sprintf(cacheInfoTable, sd.ReplaceArea()))

	var out []*api.CacheInfo
	if err := sd.cli.Select(&out, query, api.CacheStatusSuccess); err != nil {
		return nil, err
	}

	return out, nil
}

func (sd sqlDB) GetCacheInfo(cacheID string) (*api.CacheInfo, error) {
	area := sd.ReplaceArea()

	var cache api.CacheInfo
	query := fmt.Sprintf("SELECT * FROM %s WHERE device_id=?", fmt.Sprintf(cacheInfoTable, area))
	if err := sd.cli.Get(&cache, query, cacheID); err != nil {
		return nil, err
	}

	return &cache, nil
}

// remove cache info and update data info
func (sd sqlDB) RemoveCacheAndUpdateData(cacheID, carfileHash string, isDeleteData bool, reliability int) error {
	area := sd.ReplaceArea()
	cTableName := fmt.Sprintf(cacheInfoTable, area)
	dTableName := fmt.Sprintf(dataInfoTable, area)

	tx := sd.cli.MustBegin()

	// cache info
	cCmd := fmt.Sprintf(`DELETE FROM %s WHERE device_id=? `, cTableName)
	tx.MustExec(cCmd, cacheID)

	// data info
	if !isDeleteData {
		dCmd := fmt.Sprintf("UPDATE %s SET reliability=? WHERE carfile_hash=?", dTableName)
		tx.MustExec(dCmd, reliability, carfileHash)
	} else {
		dCmd := fmt.Sprintf(`DELETE FROM %s WHERE carfile_hash=?`, dTableName)
		tx.MustExec(dCmd, carfileHash)
	}

	err := tx.Commit()
	if err != nil {
		err = tx.Rollback()
		return err
	}

	return nil
}

func (sd sqlDB) UpdateCacheInfoOfQuitNode(deviceID string) (successCacheCount int, carfileReliabilitys map[string]int, err error) {
	successCacheCount = 0
	carfileReliabilitys = make(map[string]int)

	area := sd.ReplaceArea()
	cTableName := fmt.Sprintf(cacheInfoTable, area)
	dTableName := fmt.Sprintf(dataInfoTable, area)

	getCachesCmd := fmt.Sprintf("select * from %s where device_id=? and status=?", cTableName)
	var caches []*api.CacheInfo
	if err = sd.cli.Select(&caches, getCachesCmd, deviceID, api.CacheStatusSuccess); err != nil {
		return
	}

	if len(caches) <= 0 {
		err = xerrors.New(errNotFind)
		return
	}

	cacheIDs := make([]string, 0)
	for _, cache := range caches {
		cacheIDs = append(cacheIDs, cache.DeviceID)

		carfileReliabilitys[cache.CarfileHash]++
	}
	successCacheCount = len(caches)

	tx := sd.cli.MustBegin()

	updateCachesCmd := fmt.Sprintf(`UPDATE %s SET status=? WHERE device_id in (?)`, cTableName)
	query, args, err := sqlx.In(updateCachesCmd, int(api.CacheStatusRestore), cacheIDs)
	if err != nil {
		return
	}

	// cache info
	query = sd.cli.Rebind(query)
	tx.MustExec(query, args...)

	// data info
	for carfileHash, reliability := range carfileReliabilitys {
		cmdD := fmt.Sprintf(`UPDATE %s SET reliability=reliability-? WHERE carfile_hash=?`, dTableName)
		tx.MustExec(cmdD, reliability, carfileHash)
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

// cache event info
func (sd sqlDB) SetEventInfo(info *api.EventInfo) error {
	area := sd.ReplaceArea()

	tableName := fmt.Sprintf(eventInfoTable, area)

	cmd := fmt.Sprintf("INSERT INTO %s (cid, device_id, user, event, msg, device_id) VALUES (:cid, :device_id, :user, :event, :msg, :device_id)", tableName)
	_, err := sd.cli.NamedExec(cmd, info)
	return err
}

func (sd sqlDB) GetEventInfos(page int) (count int, totalPage int, out []*api.EventInfo, err error) {
	area := "cn_gd_shenzhen"
	num := 20

	cmd := fmt.Sprintf("SELECT count(cid) FROM %s ;", fmt.Sprintf(eventInfoTable, area))
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

	cmd = fmt.Sprintf("SELECT * FROM %s LIMIT %d,%d", fmt.Sprintf(eventInfoTable, area), (num * (page - 1)), num)
	if err = sd.cli.Select(&out, cmd); err != nil {
		return
	}

	return
}

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
