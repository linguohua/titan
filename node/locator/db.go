package locator

import (
	"fmt"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

// locaton config
type schedulerCfg struct {
	SchedulerURL string `db:"scheduler_url"`
	AreaID       string `db:"area_id"`
	Weight       int    `db:"weight"`
	AccessToken  string `db:"access_token"`
}

type nodeInfo struct {
	NodeID       string `db:"node_id"`
	SchedulerURL string `db:"scheduler_url"`
	AreaID       string `db:"area_id"`
	Online       bool   `db:"online"`
}

type scheduler struct {
	SchedulerURL string `db:"scheduler_url"`
	AreaID       string `db:"area_id"`
	Online       bool   `db:"online"`
}

type SqlDB struct {
	cli *sqlx.DB
}

func NewSQLDB(db *sqlx.DB) *SqlDB {
	return &SqlDB{
		cli: db,
	}
}

const nodeInfoTable = "node_info"

func (db *SqlDB) close() error {
	return db.cli.Close()
}

func (db *SqlDB) addAccessPoint(areaID string, schedulerURL string, weight int, accessToken string) error {
	return db.addSchedulerCfg(areaID, schedulerURL, weight, accessToken)
}

func (db *SqlDB) removeAccessPoints(areaID string) error {
	return db.deleteSchedulerCfgs(areaID)
}

func (db *SqlDB) getAccessPointCfgs(areaID string) ([]*schedulerCfg, error) {
	return db.getCfgs(areaID)
}

func (db *SqlDB) isAccessPointExist(schedulerURL string) (bool, error) {
	count, err := db.countSchedulerCfg(schedulerURL)
	if err != nil {
		return false, err
	}

	if count > 0 {
		return true, nil
	}

	return false, nil
}

func (db *SqlDB) listAreaIDs() (areaIDs []string, err error) {
	err = db.cli.Select(&areaIDs, `SELECT area_id FROM scheduler_config GROUP BY area_id`)
	if err != nil {
		return
	}

	return
}

func (db *SqlDB) getAllCfg() ([]*schedulerCfg, error) {
	var cfgs []*schedulerCfg
	err := db.cli.Select(&cfgs, `SELECT * FROM scheduler_config`)
	if err != nil {
		return nil, err
	}
	return cfgs, nil
}

func (db *SqlDB) getCfgs(areaID string) ([]*schedulerCfg, error) {
	var cfgs []*schedulerCfg
	err := db.cli.Select(&cfgs, `SELECT * FROM scheduler_config WHERE area_id=?`, areaID)
	if err != nil {
		return nil, err
	}
	return cfgs, nil
}

func (db *SqlDB) getSchedulerCfg(schedulerURL string) (*schedulerCfg, error) {
	cfg := &schedulerCfg{}
	err := db.cli.Get(cfg, `SELECT * FROM scheduler_config WHERE scheduler_url=?`, schedulerURL)
	if err != nil {
		return nil, err
	}

	return cfg, nil
}

func (db *SqlDB) addSchedulerCfg(areaID string, schedulerURL string, weight int, accessToken string) error {
	cfg := &schedulerCfg{SchedulerURL: schedulerURL, AreaID: areaID, Weight: weight, AccessToken: accessToken}
	_, err := db.cli.NamedExec(`INSERT INTO scheduler_config (scheduler_url, area_id, weight, access_token) VALUES (:scheduler_url, :area_id, :weight, :access_token)`, cfg)
	return err
}

func (db *SqlDB) deleteSchedulerCfgs(areaID string) error {
	cfg := &schedulerCfg{AreaID: areaID}
	_, err := db.cli.NamedExec(`DELETE FROM scheduler_config WHERE area_id=:area_id`, cfg)
	return err
}

func (db *SqlDB) deleteSchedulerCfg(schedulerURL string) error {
	cfg := &schedulerCfg{SchedulerURL: schedulerURL}
	_, err := db.cli.NamedExec(`DELETE FROM scheduler_config WHERE scheduler_url=:scheduler_url`, cfg)
	return err
}

func (db *SqlDB) getNodeInfo(nodeID string) (*nodeInfo, error) {
	devInfo := &nodeInfo{}
	cmd := fmt.Sprintf("SELECT * FROM %s WHERE node_id=?", nodeInfoTable)
	err := db.cli.Get(devInfo, cmd, nodeID)
	if err != nil {
		return nil, err
	}

	return devInfo, nil
}

func (db *SqlDB) setNodeInfo(nodeID string, schedulerURL string, areaID string, online bool) error {
	devInfo := &nodeInfo{NodeID: nodeID, SchedulerURL: schedulerURL, AreaID: areaID, Online: online}

	cmd := fmt.Sprintf(`INSERT INTO %s (node_id,scheduler_url, area_id, online) VALUES (:node_id, :scheduler_url, :area_id, :online) ON DUPLICATE KEY UPDATE scheduler_url=:scheduler_url,area_id=:area_id,online=:online`, nodeInfoTable)
	_, err := db.cli.NamedExec(cmd, devInfo)
	return err
}

func (db *SqlDB) deleteNodeInfo(nodeID string) error {
	devInfo := &nodeInfo{NodeID: nodeID}
	cmd := fmt.Sprintf(`DELETE FROM %s WHERE node_id=:node_id`, nodeInfoTable)
	_, err := db.cli.NamedExec(cmd, devInfo)
	return err
}

func (db *SqlDB) countNodeOnScheduler(schedulerURL string) (int, error) {
	var count int

	cmd := fmt.Sprintf(`select count(*) from %s WHERE scheduler_url=?`, nodeInfoTable)
	err := db.cli.Get(&count, cmd, schedulerURL)
	if err != nil {
		return 0, err
	}
	return count, err
}

func (db *SqlDB) countNodeWithID(nodeID string) (int, error) {
	var count int
	cmd := fmt.Sprintf(`select count(*) from %s WHERE node_id=?`, nodeInfoTable)
	err := db.cli.Get(&count, cmd, nodeID)
	if err != nil {
		return 0, err
	}
	return count, err
}

func (db *SqlDB) countSchedulerCfg(schedulerURL string) (int, error) {
	var count int
	err := db.cli.Get(&count, `SELECT count(*) FROM scheduler_config WHERE scheduler_url=?`, schedulerURL)
	if err != nil {
		return 0, err
	}
	return count, err
}
