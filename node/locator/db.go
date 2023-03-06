package locator

import (
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

// locaton config
type schedulerCfg struct {
	SchedulerURL string `DB:"scheduler_url"`
	AreaID       string `DB:"area_id"`
	Weight       int    `DB:"weight"`
	AccessToken  string `DB:"access_token"`
}

type deviceInfo struct {
	ID           int
	NodeID       string `DB:"node_id"`
	SchedulerURL string `DB:"scheduler_url"`
	AreaID       string `DB:"area_id"`
	Online       bool   `DB:"online"`
}

type scheduler struct {
	ID           int
	SchedulerURL string `DB:"scheduler_url"`
	AreaID       string `DB:"area_id"`
	Online       bool   `DB:"online"`
}

type SqlDB struct {
	cli *sqlx.DB
}

func NewSQLDB(db *sqlx.DB) *SqlDB {
	return &SqlDB{
		cli: db,
	}
}

//func NewSQLDB(url string) (*SqlDB, error) {
//	url = fmt.Sprintf("%s?parseTime=true&loc=Local", url)
//	db := &SqlDB{}
//	database, err := sqlx.Open("mysql", url)
//	if err != nil {
//		return nil, err
//	}
//
//	if err := database.Ping(); err != nil {
//		return nil, err
//	}
//
//	db.cli = database
//
//	return db, nil
//}

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

func (db *SqlDB) getDeviceInfo(nodeID string) (*deviceInfo, error) {
	devInfo := &deviceInfo{}
	err := db.cli.Get(devInfo, `SELECT * FROM device WHERE node_id=?`, nodeID)
	if err != nil {
		return nil, err
	}

	return devInfo, nil
}

func (db *SqlDB) setDeviceInfo(nodeID string, schedulerURL string, areaID string, online bool) error {
	devInfo := &deviceInfo{NodeID: nodeID, SchedulerURL: schedulerURL, AreaID: areaID, Online: online}
	_, err := db.cli.NamedExec(`INSERT INTO device (node_id,scheduler_url, area_id, online) VALUES (:node_id, :scheduler_url, :area_id, :online) ON DUPLICATE KEY UPDATE scheduler_url=:scheduler_url,area_id=:area_id,online=:online`, devInfo)
	return err
}

func (db *SqlDB) deleteDeviceInfo(nodeID string) error {
	devInfo := &deviceInfo{NodeID: nodeID}
	_, err := db.cli.NamedExec(`DELETE FROM device WHERE node_id=:node_id`, devInfo)
	return err
}

func (db *SqlDB) countDeviceOnScheduler(schedulerURL string) (int, error) {
	var count int
	err := db.cli.Get(&count, `select count(*) from device WHERE scheduler_url=?`, schedulerURL)
	if err != nil {
		return 0, err
	}
	return count, err
}

func (db *SqlDB) countDeviceWithID(nodeID string) (int, error) {
	var count int
	err := db.cli.Get(&count, `select count(*) from device WHERE node_id=?`, nodeID)
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
