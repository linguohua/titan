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
	blockDevicesTable = "%s"
	deviceBlockTable  = "device_blocks_%v"
)

// InitSQL init sql
func InitSQL(url string) (DB, error) {
	db := &sqlDB{url: url}
	database, err := sqlx.Open("mysql", url)
	if err != nil {
		return nil, err
	}

	if err := database.Ping(); err != nil {
		return nil, err
	}

	db.cli = database

	err = db.SetAllNodeOffline()
	return db, err
}

// IsNilErr Is NilErr
func (sd sqlDB) IsNilErr(err error) bool {
	return err.Error() == errNodeNotFind
}

func (sd sqlDB) SetNodeInfo(deviceID string, info *NodeInfo) error {
	info.DeviceID = deviceID
	info.ServerName = serverName
	info.CreateTime = time.Now().Format("2006-01-02 15:04:05")

	_, err := sd.GetNodeInfo(deviceID)
	if err != nil {
		if sd.IsNilErr(err) {
			_, err = sd.cli.NamedExec(`INSERT INTO node (device_id, last_time, geo, node_type, is_online, address, server_name, create_time)
                VALUES (:device_id, :last_time, :geo, :node_type, :is_online, :address, :server_name, :create_time)`, info)
		}
		return err
	}

	// update
	_, err = sd.cli.NamedExec(`UPDATE node SET last_time=:last_time,geo=:geo,is_online=:is_online,address=:address,server_name=:server_name  WHERE device_id=:device_id`, info)

	return err
}

// func (sd sqlDB) AddNodeOnlineTime(deviceID string, onLineTime int64) error {
// 	info := &NodeInfo{OnlineTime: onLineTime, DeviceID: deviceID}
// 	_, err := sd.cli.NamedExec(`UPDATE node SET online_time=online_time+:online_time WHERE device_id=:device_id`, info)

// 	return err
// }

// func (sd sqlDB) AddAllNodeOnlineTime(onLineTime int64) error {
// 	info := &NodeInfo{OnlineTime: onLineTime, IsOnline: 1}
// 	_, err := sd.cli.NamedExec(`UPDATE node SET online_time=online_time+:online_time WHERE is_online=:is_online`, info)

// 	return err
// }

func (sd sqlDB) SetAllNodeOffline() error {
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
	if rows.Next() {
		err = rows.StructScan(info)
		if err != nil {
			return nil, err
		}
	} else {
		return nil, xerrors.New(errNodeNotFind)
	}
	rows.Close()

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

	return xerrors.Errorf("SetValidateResultInfo err deviceid:%v ,status:%v, roundID:%v, serverName:%v", info.DeviceID, info.Status, info.RoundID, info.ServerName)
}

func (sd sqlDB) SetNodeToValidateErrorList(sID, deviceID string) error {
	_, err := sd.cli.NamedExec(`INSERT INTO validate_err (round_id, device_id)
                VALUES (:round_id, :device_id)`, map[string]interface{}{
		"round_id":  sID,
		"device_id": deviceID,
	})
	return err
}

func (sd sqlDB) RemoveBlockInfo(deviceID, cid string) error {
	info := NodeBlocks{
		CID: cid,
	}

	cmd := fmt.Sprintf(`DELETE FROM %s WHERE cid=:cid`, fmt.Sprintf(deviceBlockTable, deviceID))
	_, err := sd.cli.NamedExec(cmd, info)
	return err
}

// func (sd sqlDB) SetCarfileInfo(deviceID, cid, carfileID, cacheID string) error {
// 	info := NodeBlock{
// 		TableName: fmt.Sprintf(deviceBlockTable, deviceID),
// 		CID:       cid,
// 		CarfileID: carfileID,
// 		CacheID:   cacheID,
// 	}

// 	cmd := fmt.Sprintf(`UPDATE %s SET carfile_id=:carfile_id,cache_id=:cache_id WHERE cid=:cid`, info.TableName)
// 	_, err := sd.cli.NamedExec(cmd, info)
// 	return err
// }

func (sd sqlDB) SetBlockInfo(deviceID, cid, fid, carfileID, cacheID string, isUpdate bool) error {
	tableName := fmt.Sprintf(deviceBlockTable, deviceID)

	info := NodeBlocks{
		CID:       cid,
		FID:       fid,
		CarfileID: carfileID,
		CacheID:   cacheID,
	}

	if isUpdate {
		cmd := fmt.Sprintf(`UPDATE %s SET fid=:fid,carfile_id=:carfile_id,cache_id=:cache_id WHERE cid=:cid`, tableName)
		_, err := sd.cli.NamedExec(cmd, info)

		return err
	}

	schema := fmt.Sprintf(`
	CREATE TABLE %s (
		cid text,
		fid text,
		cache_id text,
		carfile_id text
	);`, tableName)

	_, err := sd.cli.Exec(schema)
	if err != nil {
		if !strings.Contains(err.Error(), "already exists") {
			return err
		}
	}

	cmd := fmt.Sprintf(`INSERT INTO %s (cid, fid, cache_id, carfile_id)
	VALUES (:cid, :fid, :cache_id, :carfile_id)`, tableName)

	_, err = sd.cli.NamedExec(cmd, info)

	return err
}

func (sd sqlDB) GetBlockFidWithCid(deviceID, cid string) (string, error) {
	info := &NodeBlocks{
		CID: cid,
	}

	cmd := fmt.Sprintf(`SELECT * FROM %s WHERE cid=:cid`, fmt.Sprintf(deviceBlockTable, deviceID))

	rows, err := sd.cli.NamedQuery(cmd, info)
	if err != nil {
		return "", err
	}
	if rows.Next() {
		err = rows.StructScan(info)
		if err != nil {
			return "", err
		}
	} else {
		return "", xerrors.New(errNodeNotFind)
	}
	rows.Close()

	return info.FID, err
}

func (sd sqlDB) GetBlockInfos(deviceID string) (map[string]string, error) {
	m := make(map[string]string)

	cmd := fmt.Sprintf(`SELECT * FROM %s`, fmt.Sprintf(deviceBlockTable, deviceID))

	rows, err := sd.cli.Queryx(cmd)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		i := &NodeBlocks{}
		err = rows.StructScan(i)
		if err == nil {
			m[i.CID] = i.FID
		}
	}

	return m, nil
}

func (sd sqlDB) GetBlockCidWithFid(deviceID, fid string) (string, error) {
	info := &NodeBlocks{
		FID: fid,
	}

	cmd := fmt.Sprintf(`SELECT * FROM %s WHERE fid=:fid`, fmt.Sprintf(deviceBlockTable, deviceID))
	rows, err := sd.cli.NamedQuery(cmd, info)
	if err != nil {
		return "", err
	}
	if rows.Next() {
		err = rows.StructScan(info)
		if err != nil {
			return "", err
		}
	} else {
		return "", xerrors.New(errNodeNotFind)
	}
	rows.Close()

	return info.CID, err
}

func (sd sqlDB) GetBlockNum(deviceID string) (int64, error) {
	var count int64
	cmd := fmt.Sprintf("SELECT count(*) FROM %s WHERE fid>0 ;", fmt.Sprintf(deviceBlockTable, deviceID))
	err := sd.cli.Get(&count, cmd)

	return count, err
}

func (sd sqlDB) SetDataInfo(info *DataInfo) error {
	_, err := sd.GetDataInfo(info.CID)
	if err != nil {
		if sd.IsNilErr(err) {
			_, err = sd.cli.NamedExec(`INSERT INTO data_info (cid, cache_ids, status, need_reliability)
                VALUES (:cid, :cache_ids, :status, :need_reliability)`, info)
		}
		return err
	}

	// update
	_, err = sd.cli.NamedExec(`UPDATE data_info SET cache_ids=:cache_ids,status=:status,total_size=:total_size,reliability=:reliability,cache_time=:cache_time  WHERE cid=:cid`, info)

	return err
}

func (sd sqlDB) GetDataInfo(cid string) (*DataInfo, error) {
	info := &DataInfo{CID: cid}

	rows, err := sd.cli.NamedQuery(`SELECT * FROM data_info WHERE cid=:cid`, info)
	if err != nil {
		return nil, err
	}
	if rows.Next() {
		err = rows.StructScan(info)
		if err != nil {
			return nil, err
		}
	} else {
		return nil, xerrors.New(errNodeNotFind)
	}
	rows.Close()

	return info, err
}

func (sd sqlDB) GetDataInfos() ([]*DataInfo, error) {
	list := make([]*DataInfo, 0)

	rows, err := sd.cli.Queryx(`SELECT * FROM data_info`)
	if err != nil {
		return list, err
	}
	defer rows.Close()

	for rows.Next() {
		info := &DataInfo{}
		err := rows.StructScan(info)
		if err == nil {
			list = append(list, info)
		}
	}

	return list, err
}

func (sd sqlDB) CreateCacheInfo(cacheID string) error {
	schema := fmt.Sprintf(`
	CREATE TABLE %s (
		cid text,
		device_id text,
		status integer,
		total_size integer,
		reliability integer
	);`, cacheID)

	_, err := sd.cli.Exec(schema)
	if err != nil {
		if !strings.Contains(err.Error(), "already exists") {
			return err
		}
	}

	return err
}

func (sd sqlDB) SetCacheInfo(info *CacheInfo, isUpdate bool) error {
	if isUpdate {
		cmd := fmt.Sprintf(`UPDATE %s SET device_id=:device_id,status=:status,total_size=:total_size,reliability=:reliability WHERE cid=:cid`, info.CacheID)
		_, err := sd.cli.NamedExec(cmd, info)

		return err
	}

	cmd := fmt.Sprintf(`INSERT INTO %s (cid, device_id, status, total_size, reliability)
	VALUES (:cid, :device_id, :status, :total_size, :reliability)`, info.CacheID)

	_, err := sd.cli.NamedExec(cmd, info)

	return err
}

func (sd sqlDB) GetCacheInfo(cacheID, cid string) (*CacheInfo, error) {
	info := &CacheInfo{
		CacheID: cacheID,
		CID:     cid,
	}

	cmd := fmt.Sprintf(`SELECT * FROM %s WHERE cid=:cid`, info.CacheID)

	rows, err := sd.cli.NamedQuery(cmd, info)
	if err != nil {
		return nil, err
	}
	if rows.Next() {
		err = rows.StructScan(info)
		if err != nil {
			return nil, err
		}
	} else {
		return nil, xerrors.New(errNodeNotFind)
	}
	rows.Close()

	return info, err
}

func (sd sqlDB) HaveUndoneCaches(cacheID string) (bool, error) {
	cmd := fmt.Sprintf(`SELECT * FROM %s`, cacheID)

	rows, err := sd.cli.Queryx(cmd)
	if err != nil {
		return false, err
	}
	defer rows.Close()

	for rows.Next() {
		info := &CacheInfo{}
		err := rows.StructScan(info)
		if err == nil {
			// fmt.Printf("HaveUndoneCaches info:%v", info)
			if info.Status == 1 {
				return true, nil
			}
		}
	}

	return false, nil
}

func (sd sqlDB) GetCacheInfos(cacheID string) ([]*CacheInfo, error) {
	list := make([]*CacheInfo, 0)

	cmd := fmt.Sprintf(`SELECT * FROM %s`, cacheID)

	rows, err := sd.cli.Queryx(cmd)
	if err != nil {
		return list, err
	}
	defer rows.Close()

	for rows.Next() {
		info := &CacheInfo{}
		err := rows.StructScan(info)
		if err == nil {
			list = append(list, info)
		}
	}

	return list, err
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
	if rows.Next() {
		err = rows.StructScan(info)
		if err != nil {
			return info, err
		}
	} else {
		return info, xerrors.New(errNodeNotFind)
	}
	rows.Close()

	return info, err
}

func (sd sqlDB) RemoveNodeWithCacheList(deviceID, cid string) error {
	info := BlockNodes{
		DeviceID: deviceID,
	}

	cmd := fmt.Sprintf(`DELETE FROM %s WHERE device_id=:device_id`, fmt.Sprintf(blockDevicesTable, cid))
	_, err := sd.cli.NamedExec(cmd, info)
	return err
}

func (sd sqlDB) SetNodeToCacheList(deviceID, cid string) error {
	tableName := fmt.Sprintf(blockDevicesTable, cid)
	if sd.IsNodeInCacheList(deviceID, cid) {
		return nil
	}

	info := BlockNodes{
		DeviceID: deviceID,
	}

	schema := fmt.Sprintf(`
	CREATE TABLE %s (
		device_id text
	);`, tableName)

	_, err := sd.cli.Exec(schema)
	if err != nil {
		if !strings.Contains(err.Error(), "already exists") {
			return err
		}
	}

	cmd := fmt.Sprintf(`INSERT INTO %s (device_id)
	VALUES (:device_id)`, tableName)

	_, err = sd.cli.NamedExec(cmd, info)

	return err
}

func (sd sqlDB) IsNodeInCacheList(deviceID, cid string) bool {
	info := &BlockNodes{
		DeviceID: deviceID,
	}

	cmd := fmt.Sprintf(`SELECT * FROM %s WHERE device_id=:device_id`, fmt.Sprintf(blockDevicesTable, cid))

	rows, err := sd.cli.NamedQuery(cmd, info)
	if err != nil {
		return false
	}
	if rows.Next() {
		err = rows.StructScan(info)
		if err != nil {
			return false
		}
	} else {
		return false
	}
	rows.Close()

	return true
}

func (sd sqlDB) GetNodesWithCacheList(cid string) ([]string, error) {
	m := make([]string, 0)

	cmd := fmt.Sprintf(`SELECT * FROM %s`, fmt.Sprintf(blockDevicesTable, cid))

	rows, err := sd.cli.Queryx(cmd)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		info := &BlockNodes{}
		err = rows.StructScan(info)
		if err == nil {
			m = append(m, info.DeviceID)
		}
	}

	return m, nil
}
