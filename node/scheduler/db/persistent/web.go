package persistent

import (
	"fmt"
	"time"

	"github.com/linguohua/titan/api"
)

const (
	maxCount = 500
)

type webDB interface {
	GetNodes(cursor int, count int) ([]*NodeInfo, int64, error)
	GetBlockDownloadInfos(DeviceID string, startTime time.Time, endTime time.Time, cursor, count int) ([]api.BlockDownloadInfo, int64, error)
	SetNodeConnectionLog(deviceID string, status api.NodeConnectionStatus) error
	GetNodeConnectionLogs(deviceID string, startTime time.Time, endTime time.Time, cursor, count int) ([]api.NodeConnectionLog, int64, error)

	GetCarfiles(startTime time.Time, endTime time.Time, cursor, count int) ([]api.WebCarfile, error)
	GetBlocksByCarfileID(carfileID string) ([]api.WebBlock, error)

	GetCacheStat() (api.StatCachesRsp, error)

	GetCacheTasks(startTime time.Time, endTime time.Time, cursor, count int) ([]api.CacheDataInfo, error)
	GetValidateResults(cursor int, count int) ([]api.WebValidateResult, int64, error)
}

func (sd sqlDB) GetNodes(cursor int, count int) ([]*NodeInfo, int64, error) {
	var total int64
	countSql := "SELECT count(*) FROM node"
	err := sd.cli.Get(&total, countSql)
	if err != nil {
		return nil, 0, err
	}

	queryString := "SELECT device_id FROM node limit ?,?"

	var out []*NodeInfo
	err = sd.cli.Select(&out, queryString, cursor, count)
	if err != nil {
		return nil, 0, err
	}

	return out, total, nil
}

func (sd sqlDB) GetBlockDownloadInfos(deviceID string, startTime time.Time, endTime time.Time, cursor, count int) ([]api.BlockDownloadInfo, int64, error) {
	query := fmt.Sprintf(`SELECT * FROM %s WHERE device_id = ? and created_time between ? and ? limit ?,?`,
		fmt.Sprintf(blockDownloadInfo, sd.ReplaceArea()))

	var total int64
	countSql := fmt.Sprintf(`SELECT count(*) FROM %s WHERE device_id = ? and created_time between ? and ?`, fmt.Sprintf(blockDownloadInfo, sd.ReplaceArea()))
	if err := sd.cli.Get(&total, countSql, deviceID, startTime, endTime); err != nil {
		return nil, 0, err
	}

	var out []api.BlockDownloadInfo
	if err := sd.cli.Select(&out, query, deviceID, startTime, endTime, cursor, count); err != nil {
		return nil, 0, err
	}

	return out, total, nil
}

func (sd sqlDB) SetNodeConnectionLog(deviceID string, status api.NodeConnectionStatus) error {
	log := api.NodeConnectionLog{DeviceID: deviceID, Status: status, CreatedAt: time.Now()}
	insertSql := "INSERT INTO node_connection_log (device_id, status) VALUES (:device_id, :status)"
	_, err := sd.cli.NamedExec(insertSql, log)
	return err
}

func (sd sqlDB) GetNodeConnectionLogs(deviceID string, startTime time.Time, endTime time.Time, cursor, count int) ([]api.NodeConnectionLog, int64, error) {
	var total int64
	countSql := "SELECT count(*) FROM node_connection_log WHERE device_id = ? and created_time between ? and ?"
	if err := sd.cli.Get(&total, countSql, deviceID, startTime, endTime); err != nil {
		return []api.NodeConnectionLog{}, 0, err
	}

	var query = "SELECT device_id, status, created_time FROM node_connection_log WHERE device_id = ? and created_time between ? and ? limit ?,?"
	var out []api.NodeConnectionLog
	if err := sd.cli.Select(&out, query, deviceID, startTime, endTime, cursor, count); err != nil {
		return nil, 0, err
	}
	return out, total, nil
}

func (sd sqlDB) GetCarfiles(startTime time.Time, endTime time.Time, cursor, count int) ([]api.WebCarfile, error) {
	return []api.WebCarfile{}, nil
}
func (sd sqlDB) GetBlocksByCarfileID(carfileID string) ([]api.WebBlock, error) {
	return []api.WebBlock{}, nil
}

func (sd sqlDB) GetCacheStat() (api.StatCachesRsp, error) {
	return api.StatCachesRsp{}, nil
}

func (sd sqlDB) GetCacheTasks(startTime time.Time, endTime time.Time, cursor, count int) ([]api.CacheDataInfo, error) {
	area := sd.ReplaceArea()

	query := fmt.Sprintf("SELECT * FROM %s WHERE created_time between ? and ? limit ?,?", fmt.Sprintf(dataInfoTable, area))

	var out []api.CacheDataInfo
	err := sd.cli.Select(&out, query, startTime, endTime, cursor, count)
	if err != nil {
		return nil, err
	}

	return out, nil
}

func (sd sqlDB) GetValidateResults(cursor int, count int) ([]api.WebValidateResult, int64, error) {
	var total int64
	countSql := "SELECT count(*) FROM validate_result"
	if err := sd.cli.Get(&total, countSql); err != nil {
		return []api.WebValidateResult{}, 0, err
	}

	var query = "SELECT * FROM validate_result limit ?,?"
	var out []api.WebValidateResult
	if err := sd.cli.Select(&out, query, cursor, count); err != nil {
		return nil, 0, err
	}
	return out, total, nil
}
