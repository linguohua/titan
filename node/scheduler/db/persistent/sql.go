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

const errNodeNotFind = "Not Found"

var (
	// deviceBlockTable  = "device_blocks_%s"
	eventInfoTable    = "event_info_%s"
	dataInfoTable     = "data_info_%s"
	blockInfoTable    = "block_info_%s"
	cacheInfoTable    = "cache_info_%s"
	blockDownloadInfo = "block_download_info_%s"
	blockDeleteTable  = "block_delete_%s"
)

// InitSQL init sql
func InitSQL(url string) (DB, error) {
	url = fmt.Sprintf("%s?parseTime=true", url)

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

// IsNilErr Is NilErr
func (sd sqlDB) IsNilErr(err error) bool {
	return err.Error() == errNodeNotFind
}

func (sd sqlDB) SetNodeAuthInfo(aInfo *api.DownloadServerAccessAuth) error {
	info := &NodeInfo{
		URL:         aInfo.URL,
		DeviceID:    aInfo.DeviceID,
		SecurityKey: aInfo.SecurityKey,
	}

	_, err := sd.cli.NamedExec(`UPDATE node SET security=:security,url=:url WHERE device_id=:device_id`, info)

	return err
}

func (sd sqlDB) GetNodeAuthInfo(deviceID string) (*api.DownloadServerAccessAuth, error) {
	info := &NodeInfo{DeviceID: deviceID}

	rows, err := sd.cli.NamedQuery(`SELECT security,url FROM node WHERE device_id=:device_id`, info)
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
		return nil, xerrors.New(errNodeNotFind)
	}

	return &api.DownloadServerAccessAuth{URL: info.URL, SecurityKey: info.SecurityKey, DeviceID: deviceID}, err
}

func (sd sqlDB) SetNodeInfo(deviceID string, info *NodeInfo) error {
	info.DeviceID = deviceID
	info.ServerName = serverName
	// info.CreateTime = time.Now().Format("2006-01-02 15:04:05")

	_, err := sd.GetNodeInfo(deviceID)
	if err != nil {
		if sd.IsNilErr(err) {
			_, err = sd.cli.NamedExec(`INSERT INTO node (device_id, last_time, geo, node_type, is_online, address, server_name)
                VALUES (:device_id, :last_time, :geo, :node_type, :is_online, :address, :server_name)`, info)
		}
		return err
	}

	// update
	_, err = sd.cli.NamedExec(`UPDATE node SET last_time=:last_time,geo=:geo,is_online=:is_online,address=:address,server_name=:server_name  WHERE device_id=:device_id`, info)

	return err
}

func (sd sqlDB) setAllNodeOffline() error {
	info := &NodeInfo{IsOnline: 0, ServerName: serverName}
	_, err := sd.cli.NamedExec(`UPDATE node SET is_online=:is_online WHERE server_name=:server_name`, info)

	return err
}

func (sd sqlDB) GetNodeInfo(deviceID string) (*NodeInfo, error) {
	info := &NodeInfo{DeviceID: deviceID}

	rows, err := sd.cli.NamedQuery(`SELECT * FROM node WHERE device_id=:device_id`, info)
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
		return nil, xerrors.New(errNodeNotFind)
	}

	return info, err
}

func (sd sqlDB) SetValidateResultInfo(info *ValidateResult) error {
	nowTime := time.Now().Format("2006-01-02 15:04:05")

	if ValidateStatus(info.Status) == ValidateStatusCreate || ValidateStatus(info.Status) == ValidateStatusOther {
		info.StratTime = nowTime
		info.ServerName = serverName

		_, err := sd.cli.NamedExec(`INSERT INTO validate_result (round_id, device_id, validator_id, status, strat_time, server_name, msg)
                VALUES (:round_id, :device_id, :validator_id, :status, :strat_time, :server_name, :msg)`, info)
		return err

	} else if ValidateStatus(info.Status) > ValidateStatusCreate {
		info.EndTime = nowTime

		_, err := sd.cli.NamedExec(`UPDATE validate_result SET end_time=:end_time,status=:status,msg=:msg  WHERE device_id=:device_id AND round_id=:round_id`, info)

		return err
	}

	return xerrors.Errorf("SetValidateResultInfo err deviceid:%s ,status:%d, roundID:%s, serverName:%s", info.DeviceID, info.Status, info.RoundID, info.ServerName)
}

func (sd sqlDB) SetNodeToValidateErrorList(sID, deviceID string) error {
	_, err := sd.cli.NamedExec(`INSERT INTO validate_err (round_id, device_id)
                VALUES (:round_id, :device_id)`, map[string]interface{}{
		"round_id":  sID,
		"device_id": deviceID,
	})
	return err
}

func (sd sqlDB) GetBlocksFID(deviceID string) (map[int]string, error) {
	area := sd.ReplaceArea()

	m := make(map[int]string)

	info := &BlockInfo{
		DeviceID: deviceID,
	}

	cmd := fmt.Sprintf(`SELECT * FROM %s WHERE device_id=:device_id`, fmt.Sprintf(blockInfoTable, area))
	rows, err := sd.cli.NamedQuery(cmd, info)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		i := &BlockInfo{}
		err = rows.StructScan(i)
		if err == nil && i.FID > 0 {
			m[i.FID] = i.CID
		}
	}

	return m, nil
}

func (sd sqlDB) GetBlocksInRange(startFid, endFid int, deviceID string) (map[int]string, error) {
	area := sd.ReplaceArea()

	m := make(map[int]string)

	info := &BlockInfo{
		DeviceID: deviceID,
	}

	cmd := fmt.Sprintf(`SELECT cid,fid FROM %s WHERE device_id=:device_id and status=%d and fid between %d and %d`, fmt.Sprintf(blockInfoTable, area), CacheStatusSuccess, startFid, endFid)
	rows, err := sd.cli.NamedQuery(cmd, info)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		i := &BlockInfo{}
		err = rows.StructScan(i)
		if err == nil && i.FID > 0 {
			m[i.FID] = i.CID
		}
	}

	return m, nil
}

func (sd sqlDB) GetBlocksBiggerThan(startFid int, deviceID string) (map[int]string, error) {
	area := sd.ReplaceArea()

	m := make(map[int]string)

	info := &BlockInfo{
		DeviceID: deviceID,
	}

	cmd := fmt.Sprintf(`SELECT cid,fid FROM %s WHERE device_id=:device_id and status=%d and fid > %d`, fmt.Sprintf(blockInfoTable, area), CacheStatusSuccess, startFid)
	rows, err := sd.cli.NamedQuery(cmd, info)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		i := &BlockInfo{}
		err = rows.StructScan(i)
		if err == nil && i.FID > 0 {
			m[i.FID] = i.CID
		}
	}

	return m, nil
}

// func (sd sqlDB) GetNodeBlock(deviceID, cid string) ([]*BlockInfo, error) {
// 	area := sd.ReplaceArea()

// 	list := make([]*BlockInfo, 0)

// 	info := &BlockInfo{
// 		CID:      cid,
// 		DeviceID: deviceID,
// 	}

// 	cmd := fmt.Sprintf(`SELECT * FROM %s WHERE cid=:cid AND device_id=:device_id`, fmt.Sprintf(blockInfoTable, area))
// 	rows, err := sd.cli.NamedQuery(cmd, info)
// 	if err != nil {
// 		return nil, err
// 	}
// 	defer rows.Close()

// 	for rows.Next() {
// 		i := &BlockInfo{}
// 		err = rows.StructScan(i)
// 		if err == nil {
// 			list = append(list, i)
// 		}
// 	}

// 	return list, nil
// }

func (sd sqlDB) GetBlockCidWithFid(deviceID string, fid int) (string, error) {
	area := sd.ReplaceArea()

	info := &BlockInfo{
		FID:      fid,
		DeviceID: deviceID,
	}

	cmd := fmt.Sprintf(`SELECT * FROM %s WHERE fid=:fid AND device_id=:device_id`, fmt.Sprintf(blockInfoTable, area))
	rows, err := sd.cli.NamedQuery(cmd, info)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	if rows.Next() {
		err = rows.StructScan(info)
		if err != nil {
			return "", err
		}
	} else {
		return "", xerrors.New(errNodeNotFind)
	}

	return info.CID, err
}

func (sd sqlDB) GetDeviceBlockNum(deviceID string) (int64, error) {
	area := sd.ReplaceArea()

	var count int64
	cmd := fmt.Sprintf("SELECT count(id) FROM %s WHERE device_id=? AND status=? ;", fmt.Sprintf(blockInfoTable, area))
	err := sd.cli.Get(&count, cmd, deviceID, CacheStatusSuccess)

	return count, err
}

func (sd sqlDB) RemoveCacheInfo(cacheID, carfileID string, isDeleteData bool, reliability int) error {
	area := sd.ReplaceArea()
	cTableName := fmt.Sprintf(cacheInfoTable, area)
	dTableName := fmt.Sprintf(dataInfoTable, area)
	bTableName := fmt.Sprintf(blockInfoTable, area)

	tx := sd.cli.MustBegin()

	// cache info
	cCmd := fmt.Sprintf(`DELETE FROM %s WHERE cache_id=? `, cTableName)
	tx.MustExec(cCmd, cacheID)

	// data info
	if !isDeleteData {
		dCmd := fmt.Sprintf("UPDATE %s SET reliability=? WHERE cid=?", dTableName)
		tx.MustExec(dCmd, reliability, carfileID)
	} else {
		dCmd := fmt.Sprintf(`DELETE FROM %s WHERE cid=?`, dTableName)
		tx.MustExec(dCmd, carfileID)
	}

	// delete device block info
	// dCmd := fmt.Sprintf(`DELETE FROM %s WHERE cache_id=?`, fmt.Sprintf(deviceBlockTable, area))
	// tx.MustExec(dCmd, cacheID)

	// delete block info
	bCmd := fmt.Sprintf(`DELETE FROM %s WHERE cache_id=?`, bTableName)
	tx.MustExec(bCmd, cacheID)

	err := tx.Commit()
	if err != nil {
		err = tx.Rollback()
		return err
	}

	return nil
}

func (sd sqlDB) CreateCache(cInfo *CacheInfo) error {
	area := sd.ReplaceArea()
	cTableName := fmt.Sprintf(cacheInfoTable, area)
	// dTableName := fmt.Sprintf(dataInfoTable, area)

	tx := sd.cli.MustBegin()

	// cache info
	cCmd := fmt.Sprintf("INSERT INTO %s (carfile_cid, cache_id, status, expired_time, root_cache) VALUES (?, ?, ?, ?, ?)", cTableName)
	tx.MustExec(cCmd, cInfo.CarfileCid, cInfo.CacheID, cInfo.Status, cInfo.ExpiredTime, cInfo.RootCache)

	// data info
	// dCmd := fmt.Sprintf("UPDATE %s SET cache_ids=? WHERE cid=?", dTableName)
	// tx.MustExec(dCmd, dInfo.CacheIDs, dInfo.CID)

	err := tx.Commit()
	if err != nil {
		err = tx.Rollback()
		return err
	}

	// info, err := sd.GetCacheInfo(cInfo.CacheID, cInfo.CarfileID)
	// if err != nil {
	// 	return 0, err
	// }

	return nil
}

func (sd sqlDB) SaveCacheEndResults(dInfo *DataInfo, cInfo *CacheInfo) error {
	area := sd.ReplaceArea()
	dTableName := fmt.Sprintf(dataInfoTable, area)
	cTableName := fmt.Sprintf(cacheInfoTable, area)

	tx := sd.cli.MustBegin()

	// data info
	dCmd := fmt.Sprintf("UPDATE %s SET total_size=?,reliability=?,cache_count=?,total_blocks=?,nodes=? WHERE carfile_cid=?", dTableName)
	tx.MustExec(dCmd, dInfo.TotalSize, dInfo.Reliability, dInfo.CacheCount, dInfo.TotalBlocks, dInfo.Nodes, dInfo.CarfileCid)

	// cache info
	cCmd := fmt.Sprintf(`UPDATE %s SET done_size=?,done_blocks=?,reliability=?,status=?,total_size=?,total_blocks=?,nodes=? WHERE cache_id=? `, cTableName)
	tx.MustExec(cCmd, cInfo.DoneSize, cInfo.DoneBlocks, cInfo.Reliability, cInfo.Status, cInfo.TotalSize, cInfo.TotalBlocks, cInfo.Nodes, cInfo.CacheID)

	err := tx.Commit()
	if err != nil {
		err = tx.Rollback()
	}

	return err
}

func (sd sqlDB) SaveCacheingResults(dInfo *DataInfo, cInfo *CacheInfo, updateBlock *BlockInfo, createBlocks []*BlockInfo) error {
	area := sd.ReplaceArea()
	bTableName := fmt.Sprintf(blockInfoTable, area)
	dTableName := fmt.Sprintf(dataInfoTable, area)
	cTableName := fmt.Sprintf(cacheInfoTable, area)

	tx := sd.cli.MustBegin()

	// data info
	dCmd := fmt.Sprintf("UPDATE %s SET total_size=?,reliability=?,cache_count=?,total_blocks=? WHERE carfile_cid=?", dTableName)
	tx.MustExec(dCmd, dInfo.TotalSize, dInfo.Reliability, dInfo.CacheCount, dInfo.TotalBlocks, dInfo.CarfileCid)

	// cache info
	cCmd := fmt.Sprintf(`UPDATE %s SET done_size=?,done_blocks=?,reliability=?,status=?,total_size=?,total_blocks=? WHERE cache_id=? `, cTableName)
	tx.MustExec(cCmd, cInfo.DoneSize, cInfo.DoneBlocks, cInfo.Reliability, cInfo.Status, cInfo.TotalSize, cInfo.TotalBlocks, cInfo.CacheID)

	// block info
	bCmd := fmt.Sprintf(`UPDATE %s SET status=?,size=?,reliability=?,device_id=? WHERE id=?`, bTableName)
	tx.MustExec(bCmd, updateBlock.Status, updateBlock.Size, updateBlock.Reliability, updateBlock.DeviceID, updateBlock.ID)

	// if fid != "" {
	// 	cmd1 := fmt.Sprintf(`INSERT INTO %s (cid, fid, cache_id, carfile_cid, device_id) VALUES (?, ?, ?, ?, ?)`, fmt.Sprintf(deviceBlockTable, area))
	// 	tx.MustExec(cmd1, updateBlock.CID, fid, updateBlock.CacheID, cInfo.CarfileID, updateBlock.DeviceID)
	// }

	if createBlocks != nil {
		for _, info := range createBlocks {
			if info.ID != "" {
				cmd := fmt.Sprintf(`UPDATE %s SET status=?,size=?,reliability=?,device_id=?,fid=? WHERE id=?`, bTableName)
				tx.MustExec(cmd, info.Status, info.Size, info.Reliability, info.DeviceID, info.FID, info.ID)
			} else {
				cmd := fmt.Sprintf(`INSERT INTO %s (cache_id, carfile_cid, cid, device_id, status, size, reliability, id, fid) VALUES (?, ?, ?, ?, ?, ?, ?, REPLACE(UUID(),"-",""), ?)`, bTableName)
				tx.MustExec(cmd, info.CacheID, info.CarfileCid, info.CID, info.DeviceID, info.Status, info.Size, info.Reliability, info.FID)
			}
		}
	}

	err := tx.Commit()
	if err != nil {
		err = tx.Rollback()
	}

	return err
}

func (sd sqlDB) SetDataInfo(info *DataInfo) error {
	area := sd.ReplaceArea()

	tableName := fmt.Sprintf(dataInfoTable, area)

	oldInfo, err := sd.GetDataInfo(info.CarfileCid)
	if err != nil {
		return err
	}

	if oldInfo == nil {
		cmd := fmt.Sprintf("INSERT INTO %s (cid, status, need_reliability, total_blocks, expired_time) VALUES (:cid, :status, :need_reliability, :total_blocks, :expired_time)", tableName)
		_, err = sd.cli.NamedExec(cmd, info)
		return err
	}

	// update
	cmd := fmt.Sprintf("UPDATE %s SET expired_time=:expired_time,status=:status,total_size=:total_size,reliability=:reliability,cache_count=:cache_count,total_blocks=:total_blocks,need_reliability=:need_reliability WHERE cid=:cid", tableName)
	_, err = sd.cli.NamedExec(cmd, info)

	return err
}

func (sd sqlDB) GetDataInfo(cid string) (*DataInfo, error) {
	area := sd.ReplaceArea()

	info := &DataInfo{CarfileCid: cid}

	cmd := fmt.Sprintf("SELECT * FROM %s WHERE carfile_cid=:carfile_cid", fmt.Sprintf(dataInfoTable, area))

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
		return nil, nil
	}

	return info, err
}

func (sd sqlDB) GetDataCidWithPage(page int) (count int, totalPage int, list []string, err error) {
	area := sd.ReplaceArea()
	p := 20

	cmd := fmt.Sprintf("SELECT count(cid) FROM %s ;", fmt.Sprintf(dataInfoTable, area))
	err = sd.cli.Get(&count, cmd)
	if err != nil {
		return
	}

	totalPage = count / p
	if count%p > 0 {
		totalPage++
	}

	if page > totalPage {
		err = xerrors.Errorf("totalPage:%d but page:%d", totalPage, page)
		return
	}

	// select * from table order by id limit m, n;
	// cmd = fmt.Sprintf("SELECT * FROM %s WHERE id>=(SELECT id FROM %s order by id limit %d,1) LIMIT %d", fmt.Sprintf(dataInfoTable, area), fmt.Sprintf(dataInfoTable, area), (p * (page - 1)), p)
	cmd = fmt.Sprintf("SELECT carfile_cid FROM %s LIMIT %d,%d", fmt.Sprintf(dataInfoTable, area), (p * (page - 1)), p)
	rows, err := sd.cli.Queryx(cmd)
	if err != nil {
		return
	}
	defer rows.Close()

	list = make([]string, 0)
	for rows.Next() {
		info := &DataInfo{}
		err := rows.StructScan(info)

		if err == nil {
			list = append(list, info.CarfileCid)
		}
	}

	return
}

func (sd sqlDB) GetCachesWithData(cid string) ([]string, error) {
	area := sd.ReplaceArea()

	list := make([]string, 0)

	i := &CacheInfo{CarfileCid: cid}

	cmd := fmt.Sprintf(`SELECT cache_id FROM %s WHERE carfile_cid=:carfile_cid`, fmt.Sprintf(cacheInfoTable, area))
	rows, err := sd.cli.NamedQuery(cmd, i)
	if err != nil {
		return list, err
	}
	defer rows.Close()

	for rows.Next() {
		info := &CacheInfo{}
		err := rows.StructScan(info)
		if err == nil {
			list = append(list, info.CacheID)
		}
	}

	return list, err
}

func (sd sqlDB) GetCacheInfo(cacheID string) (*CacheInfo, error) {
	area := sd.ReplaceArea()

	info := &CacheInfo{
		CacheID: cacheID,
	}

	cmd := fmt.Sprintf(`SELECT * FROM %s WHERE cache_id=:cache_id`, fmt.Sprintf(cacheInfoTable, area))

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
		return nil, nil
	}

	return info, err
}

func (sd sqlDB) SetBlockInfos(infos []*BlockInfo, carfileCid string) error {
	area := sd.ReplaceArea()
	tableName := fmt.Sprintf(blockInfoTable, area)

	tx := sd.cli.MustBegin()

	for _, info := range infos {
		if info.ID != "" {
			cmd := fmt.Sprintf(`UPDATE %s SET status=?,size=?,reliability=?,device_id=?,fid=? WHERE id=?`, tableName)
			tx.MustExec(cmd, info.Status, info.Size, info.Reliability, info.DeviceID, info.FID, info.ID)

			// cmd1 := fmt.Sprintf(`UPDATE %s SET device_id=? WHERE cid=? AND cache_id=? AND carfile_cid=?,`, fmt.Sprintf(deviceBlockTable, area))
			// tx.MustExec(cmd1, info.DeviceID, info.CID, info.CacheID, carfileCid)
		} else {
			cmd := fmt.Sprintf(`INSERT INTO %s (cache_id, carfile_cid, cid, device_id, status, size, reliability, fid, id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, REPLACE(UUID(),"-",""))`, tableName)
			tx.MustExec(cmd, info.CacheID, info.CarfileCid, info.CID, info.DeviceID, info.Status, info.Size, info.Reliability, info.FID)

			// cmd1 := fmt.Sprintf(`INSERT INTO %s (cid, fid, cache_id, carfile_cid, device_id) VALUES (?, ?, ?, ?, ?)`, fmt.Sprintf(deviceBlockTable, area))
			// tx.MustExec(cmd1, info.CID, "", info.CacheID, carfileCid, info.DeviceID)
		}
	}

	err := tx.Commit()
	if err != nil {
		err = tx.Rollback()
	}

	return err
}

func (sd sqlDB) GetBlockInfo(cacheID, cid, deviceID string) (*BlockInfo, error) {
	area := sd.ReplaceArea()

	info := &BlockInfo{
		CacheID:  cacheID,
		CID:      cid,
		DeviceID: deviceID,
	}

	cmd := fmt.Sprintf(`SELECT * FROM %s WHERE cache_id=:cache_id AND cid=:cid AND device_id=:device_id`, fmt.Sprintf(blockInfoTable, area))

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
		return nil, nil
	}

	return info, err
}

func (sd sqlDB) GetUndoneBlocks(cacheID string) (map[string]string, error) {
	area := sd.ReplaceArea()

	list := make(map[string]string, 0)

	i := &BlockInfo{CacheID: cacheID, Status: int(CacheStatusSuccess)}

	cmd := fmt.Sprintf(`SELECT * FROM %s WHERE cache_id=:cache_id AND status!=:status`, fmt.Sprintf(blockInfoTable, area))
	rows, err := sd.cli.NamedQuery(cmd, i)
	if err != nil {
		return list, err
	}
	defer rows.Close()

	for rows.Next() {
		info := &BlockInfo{}
		err := rows.StructScan(info)
		if err == nil {
			list[info.CID] = info.ID
			// list = append(list, info.CID)
		}
	}

	return list, err
}

func (sd sqlDB) GetAllBlocks(cacheID string) (map[string][]string, error) {
	area := sd.ReplaceArea()

	list := make(map[string][]string, 0)

	i := &BlockInfo{CacheID: cacheID}

	cmd := fmt.Sprintf(`SELECT * FROM %s WHERE cache_id=:cache_id`, fmt.Sprintf(blockInfoTable, area))
	rows, err := sd.cli.NamedQuery(cmd, i)
	if err != nil {
		return list, err
	}
	defer rows.Close()

	for rows.Next() {
		info := &BlockInfo{}
		err := rows.StructScan(info)
		if err == nil {
			cids, ok := list[info.DeviceID]
			if !ok {
				cids = make([]string, 0)
			}
			cids = append(cids, info.CID)
			list[info.DeviceID] = cids
			// list = append(list, info.CID)
		}
	}

	return list, err
}

func (sd sqlDB) GetBloackCountWithStatus(cacheID string, status int) (int, error) {
	area := sd.ReplaceArea()

	var count int
	cmd := fmt.Sprintf("SELECT count(id) FROM %s WHERE cache_id=? AND status=?;", fmt.Sprintf(blockInfoTable, area))
	err := sd.cli.Get(&count, cmd, cacheID, status)

	return count, err

	// rows, err := sd.cli.NamedQuery(cmd, info)
	// if err != nil {
	// 	return false, err
	// }
	// defer rows.Close()

	// if rows.Next() {
	// 	return true, nil
	// }

	// return false, nil
}

func (sd sqlDB) GetCachesSize(cacheID string, status int) (int, error) {
	area := sd.ReplaceArea()

	var count int
	cmd := fmt.Sprintf("SELECT sum(size) FROM %s WHERE cache_id=? AND status=?;", fmt.Sprintf(blockInfoTable, area))
	err := sd.cli.Get(&count, cmd, cacheID, status)

	return count, err
}

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

func (sd sqlDB) GetRegisterInfo(deviceID string) (*api.NodeRegisterInfo, error) {
	info := &api.NodeRegisterInfo{
		DeviceID: deviceID,
	}

	rows, err := sd.cli.NamedQuery(`SELECT * FROM register WHERE device_id=:device_id`, info)
	if err != nil {
		return info, err
	}
	defer rows.Close()
	if rows.Next() {
		err = rows.StructScan(info)
		if err != nil {
			return info, err
		}
	} else {
		return info, xerrors.New(errNodeNotFind)
	}

	return info, err
}

func (sd sqlDB) GetNodesWithCache(cid string, isSuccess bool) ([]string, error) {
	area := sd.ReplaceArea()

	list := make([]string, 0)

	info := &BlockInfo{
		CID:    cid,
		Status: int(CacheStatusSuccess),
	}

	cmd := fmt.Sprintf(`SELECT * FROM %s WHERE cid=:cid `, fmt.Sprintf(blockInfoTable, area))
	if isSuccess {
		cmd = fmt.Sprintf(`SELECT * FROM %s WHERE cid=:cid AND status=:status`, fmt.Sprintf(blockInfoTable, area))
	}

	rows, err := sd.cli.NamedQuery(cmd, info)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		i := &BlockInfo{}
		err = rows.StructScan(i)
		if err != nil {
			return list, err
		}
		list = append(list, i.DeviceID)
	}

	return list, nil
}

// func (sd sqlDB) DeleteBlockInfos(cacheID, deviceID string, cids []string, removeBlocks int) error {
// 	area := sd.ReplaceArea()
// 	bTableName := fmt.Sprintf(blockInfoTable, area)
// 	cTableName := fmt.Sprintf(cacheInfoTable, area)
// 	if cids == nil || len(cids) < 0 {
// 		return nil
// 	}

// 	tx := sd.cli.MustBegin()
// 	for _, cid := range cids {
// 		// delete device block info
// 		dCmd := fmt.Sprintf(`DELETE FROM %s WHERE cid=? AND device_id=? AND cache_id=?`, fmt.Sprintf(deviceBlockTable, area))
// 		tx.MustExec(dCmd, cid, deviceID, cacheID)
// 		// delete block info
// 		bCmd := fmt.Sprintf(`DELETE FROM %s WHERE cid=? AND device_id=? AND cache_id=?`, bTableName)
// 		tx.MustExec(bCmd, cid, deviceID, cacheID)
// 	}

// 	cCmd := fmt.Sprintf("UPDATE %s SET remove_blocks=? WHERE cache_id=?", cTableName)
// 	tx.MustExec(cCmd, removeBlocks+len(cids), cacheID)

// 	err := tx.Commit()
// 	if err != nil {
// 		err = tx.Rollback()
// 	}

// 	return err
// }

func (sd sqlDB) ReplaceArea() string {
	str := strings.ToLower(serverArea)
	str = strings.Replace(str, "-", "_", -1)

	return str
}

func (sd sqlDB) GetNodesFromData(cid string) (int, error) {
	area := sd.ReplaceArea()

	i := &BlockInfo{CarfileCid: cid}

	cmd := fmt.Sprintf("SELECT DISTINCT device_id FROM %s WHERE carfile_cid=:carfile_cid", fmt.Sprintf(blockInfoTable, area))

	rows, err := sd.cli.NamedQuery(cmd, i)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	devices := 0

	for rows.Next() {
		info := &BlockInfo{}
		err := rows.StructScan(info)
		if err == nil {
			devices++
		}
	}

	return devices, nil
}

func (sd sqlDB) GetNodesFromCache(cacheID string) (int, error) {
	area := sd.ReplaceArea()

	i := &BlockInfo{CacheID: cacheID}

	cmd := fmt.Sprintf("SELECT DISTINCT device_id FROM %s WHERE cache_id=:cache_id", fmt.Sprintf(blockInfoTable, area))
	rows, err := sd.cli.NamedQuery(cmd, i)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	devices := 0

	for rows.Next() {
		info := &BlockInfo{}
		err := rows.StructScan(info)
		if err == nil {
			devices++
		}
	}

	return devices, nil
}

func (sd sqlDB) AddDownloadInfo(deviceID string, info *api.BlockDownloadInfo) error {
	query := fmt.Sprintf(
		`INSERT INTO %s (block_cid, device_id, block_size, speed, reward) 
				VALUES (:block_cid, :device_id, :block_size, :speed, :reward)`, fmt.Sprintf(blockDownloadInfo, sd.ReplaceArea()))

	_, err := sd.cli.NamedExec(query, info)
	if err != nil {
		return err
	}

	return nil
}

func (sd sqlDB) GetDownloadInfo(deviceID string) ([]*api.BlockDownloadInfo, error) {
	query := fmt.Sprintf(`SELECT * FROM %s WHERE device_id = ? and created_time >= TO_DAYS(NOW()) ORDER BY created_time DESC`,
		fmt.Sprintf(blockDownloadInfo, sd.ReplaceArea()))

	var out []*api.BlockDownloadInfo
	if err := sd.cli.Select(&out, query, deviceID); err != nil {
		return nil, err
	}

	return out, nil
}

func (sd sqlDB) SetEventInfo(info *EventInfo) error {
	area := sd.ReplaceArea()

	tableName := fmt.Sprintf(eventInfoTable, area)

	cmd := fmt.Sprintf("INSERT INTO %s (cid, device_id, user, event, msg, cache_id) VALUES (:cid, :device_id, :user, :event, :msg, :cache_id)", tableName)
	_, err := sd.cli.NamedExec(cmd, info)
	return err
}

// func (sd sqlDB) AddToBeDeleteBlock(infos []*BlockDelete) error {
// 	area := sd.ReplaceArea()
// 	tableName := fmt.Sprintf(blockDeleteTable, area)

// 	tx := sd.cli.MustBegin()

// 	for _, info := range infos {
// 		cCmd := fmt.Sprintf("INSERT INTO %s (cid, device_id, cache_id, msg) VALUES (?, ?, ?, ?)", tableName)
// 		tx.MustExec(cCmd, info.CID, info.DeviceID, info.CacheID, info.Msg)
// 	}

// 	err := tx.Commit()
// 	if err != nil {
// 		err = tx.Rollback()
// 		return err
// 	}

// 	return nil
// }

// func (sd sqlDB) GetToBeDeleteBlocks(deviceID string) ([]*BlockDelete, error) {
// 	area := sd.ReplaceArea()

// 	list := make([]*BlockDelete, 0)

// 	info := &BlockDelete{
// 		DeviceID: deviceID,
// 	}

// 	cmd := fmt.Sprintf(`SELECT * FROM %s WHERE device_id=:device_id`, fmt.Sprintf(blockDeleteTable, area))
// 	rows, err := sd.cli.NamedQuery(cmd, info)
// 	if err != nil {
// 		return nil, err
// 	}
// 	defer rows.Close()

// 	for rows.Next() {
// 		i := &BlockDelete{}
// 		err = rows.StructScan(i)
// 		if err == nil {
// 			list = append(list, i)
// 		}
// 	}

// 	return list, nil
// }

// func (sd sqlDB) RemoveToBeDeleteBlock(infos []*BlockDelete) error {
// 	area := sd.ReplaceArea()
// 	tableName := fmt.Sprintf(blockDeleteTable, area)

// 	tx := sd.cli.MustBegin()

// 	for _, info := range infos {
// 		cCmd := fmt.Sprintf(`DELETE FROM %s WHERE cid=? AND device_id=?`, tableName)
// 		tx.MustExec(cCmd, info.CID, info.DeviceID)
// 	}

// 	err := tx.Commit()
// 	if err != nil {
// 		err = tx.Rollback()
// 		return err
// 	}

// 	return nil
// }
