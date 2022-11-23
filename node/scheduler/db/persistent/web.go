package persistent

import (
	"fmt"

	"github.com/linguohua/titan/api"
)

type NodeConnectionActionType int

const (
	ActionTypeUnknow NodeConnectionActionType = iota
	ActionTypeOnline
	ActionTypeOffline
)

type nodeConnectionLog struct {
	DeviceID   string
	ActionType int
	ActionTime int64
}

type webDB interface {
	GetNodes(cursor int, count int) ([]*NodeInfo, int64, error)
	GetBlockDownloadInfos(DeviceID string, startTime int64, endTime int64, cursor, count int) ([]api.BlockDownloadInfo, int64, error)
}

func (sd sqlDB) CountNodes() (int64, error) {
	var count int64
	queryString := "SELECT count(*) FROM node"
	err := sd.cli.Get(&count, queryString)

	return count, err
}

func (sd sqlDB) GetNodes(cursor int, count int) ([]*NodeInfo, int64, error) {
	var total int64
	queryString := "SELECT count(*) FROM node"
	err := sd.cli.Get(&total, queryString)
	if err != nil {
		return nil, 0, err
	}

	queryString = "SELECT device_id FROM node limit ?,?"
	rows, err := sd.cli.Queryx(queryString, cursor, count)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	nodes := make([]*NodeInfo, 0)
	for rows.Next() {
		node := &NodeInfo{}
		err = rows.StructScan(node)
		if err != nil {
			return nil, 0, err
		}

		nodes = append(nodes, node)
	}

	return nodes, total, nil
}

func (sd sqlDB) GetBlockDownloadInfos(deviceID string, startTime int64, endTime int64, cursor, count int) ([]api.BlockDownloadInfo, int64, error) {
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
