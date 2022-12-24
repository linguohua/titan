package locator

import (
	"fmt"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/linguohua/titan/node/config"
)

type db struct {
	db *sqlDB
}

// locaton config
type locatorCfg struct {
	ID           int
	SchedulerURL string `db:"scheduler_url"`
	AreaID       string `db:"area_id"`
	Weight       int    `db:"weight"`
	AccessToken  string `db:"access_token"`
}

type deviceInfo struct {
	ID           int
	DeviceID     string `db:"device_id"`
	SchedulerURL string `db:"scheduler_url"`
	AreaID       string `db:"area_id"`
	Online       bool   `db:"online"`
}

type scheduler struct {
	ID           int
	SchedulerURL string `db:"scheduler_url"`
	AreaID       string `db:"area_id"`
	Online       bool   `db:"online"`
}

// `device_id` varchar(128) NOT NULL UNIQUE ,
//     `scheduler_url` varchar(128) DEFAULT NULL ,
//     `area_id` varchar(64) DEFAULT NULL ,
//     `onlilne` TINYINT  DEFAULT '0'  ,

func newDB(dbAddr string) *db {
	cli, err := initSQLDB(dbAddr)
	if err != nil {
		log.Panicf("newDB error:%s", err.Error())
		return nil
	}

	return &db{cli}
}

func (db *db) addAccessPoints(areaID string, schedulerURL string, weight int, accessToken string) error {
	return db.db.addCfg(areaID, schedulerURL, weight, accessToken)
}
func (db *db) removeAccessPoints(areaID string) error {
	return db.db.DeleteCfgWithAreaID(areaID)
}

func (db *db) listAccessPoints() (areaIDs []string, err error) {
	allCfg, err := db.db.getAllCfg()
	if err != nil {
		return []string{}, err
	}
	cfgMap := make(map[string]string)
	for _, cfg := range allCfg {
		cfgMap[cfg.AreaID] = cfg.SchedulerURL
	}

	for k := range cfgMap {
		areaIDs = append(areaIDs, k)
	}
	return areaIDs, nil
}

func (db *db) getAccessPoint(areaID string) (*config.AccessPoint, error) {
	cfgs, err := db.db.getCfgs(areaID)
	if err != nil {
		return nil, err
	}
	ap := config.AccessPoint{AreaID: areaID, SchedulerCfgs: make([]config.SchedulerCfg, 0, len(cfgs))}
	for _, cfg := range cfgs {
		schedulerCfg := config.SchedulerCfg{URL: cfg.SchedulerURL, Weight: cfg.Weight, AccessToken: cfg.AccessToken}
		ap.SchedulerCfgs = append(ap.SchedulerCfgs, schedulerCfg)
	}

	return &ap, nil
}

func (db *db) isAccessPointExist(areaID, schedulerURL string) (bool, error) {
	count, err := db.db.countCfgWith(areaID, schedulerURL)
	if err != nil {
		return false, err
	}

	if count > 0 {
		return true, nil
	}

	return false, nil
}

func (db *db) countDeviceOnScheduler(schedulerURL string) (int, error) {
	return db.db.countDeviceOnScheduler(schedulerURL)
}

func (db *db) getDeviceInfo(deviceID string) (*deviceInfo, error) {
	return db.db.getDeviceInfo(deviceID)
}

func (db *db) close() error {
	return db.db.cli.Close()
}

type sqlDB struct {
	cli *sqlx.DB
}

func initSQLDB(url string) (*sqlDB, error) {
	url = fmt.Sprintf("%s?parseTime=true&loc=Local", url)
	db := &sqlDB{}
	database, err := sqlx.Open("mysql", url)
	if err != nil {
		return nil, err
	}

	if err := database.Ping(); err != nil {
		return nil, err
	}

	db.cli = database

	return db, nil
}

func (db *sqlDB) getAllCfg() ([]*locatorCfg, error) {
	cfg := &locatorCfg{}
	rows, err := db.cli.NamedQuery(`SELECT * FROM config`, cfg)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	cfgs := make([]*locatorCfg, 0)
	for rows.Next() {
		var cfg locatorCfg
		err = rows.StructScan(&cfg)
		if err != nil {
			return nil, err
		}
		cfgs = append(cfgs, &cfg)
	}

	return cfgs, nil
}

func (db *sqlDB) getCfgs(areaID string) ([]*locatorCfg, error) {
	cfg := &locatorCfg{AreaID: areaID}
	rows, err := db.cli.NamedQuery(`SELECT * FROM config WHERE area_id=:area_id`, cfg)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	cfgs := make([]*locatorCfg, 0)
	for rows.Next() {
		cfg := locatorCfg{}
		err = rows.StructScan(&cfg)
		if err != nil {
			return nil, err
		}
		cfgs = append(cfgs, &cfg)
	}

	return cfgs, nil
}

func (db *sqlDB) addCfg(areaID string, schedulerURL string, weight int, accessToken string) error {
	cfg := &locatorCfg{SchedulerURL: schedulerURL, AreaID: areaID, Weight: weight, AccessToken: accessToken}
	_, err := db.cli.NamedExec(`INSERT INTO config (scheduler_url, area_id, weight, access_token) VALUES (:scheduler_url, :area_id, :weight, :access_token)`, cfg)
	return err
}

func (db *sqlDB) DeleteCfgWithAreaID(areaID string) error {
	cfg := &locatorCfg{AreaID: areaID}
	_, err := db.cli.NamedExec(`DELETE FROM config WHERE area_id=:area_id`, cfg)
	return err
}

func (db *sqlDB) DeleteCfgWithURL(schedulerURL string) error {
	cfg := &locatorCfg{SchedulerURL: schedulerURL}
	_, err := db.cli.NamedExec(`DELETE FROM config WHERE scheduler_url=:scheduler_url`, cfg)
	return err
}

func (db *sqlDB) getDeviceInfo(deviceID string) (*deviceInfo, error) {
	devInfo := &deviceInfo{DeviceID: deviceID}
	rows, err := db.cli.NamedQuery(`SELECT * FROM device WHERE device_id=:device_id`, devInfo)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	devInfos := make([]*deviceInfo, 0)
	for rows.Next() {
		info := deviceInfo{}
		err = rows.StructScan(&info)
		if err != nil {
			return nil, err
		}
		devInfos = append(devInfos, &info)
	}

	if len(devInfos) > 0 {
		return devInfos[0], nil
	}

	return nil, nil
}

func (db *sqlDB) setDeviceInfo(deviceID string, schedulerURL string, areaID string, online bool) error {
	devInfo := &deviceInfo{DeviceID: deviceID, SchedulerURL: schedulerURL, AreaID: areaID, Online: online}
	_, err := db.cli.NamedExec(`INSERT INTO device (device_id,scheduler_url, area_id, online) VALUES (:device_id, :scheduler_url, :area_id, :online) ON DUPLICATE KEY UPDATE scheduler_url=:scheduler_url,area_id=:area_id,online=:online`, devInfo)
	return err
}

func (db *sqlDB) deleteDeviceInfo(deviceID string) error {
	devInfo := &deviceInfo{DeviceID: deviceID}
	_, err := db.cli.NamedExec(`DELETE FROM device WHERE device_id=:device_id`, devInfo)
	return err
}

func (db *sqlDB) countDeviceOnScheduler(schedulerURL string) (int, error) {
	var count int
	err := db.cli.Get(&count, `select count(*) from device WHERE scheduler_url=?`, schedulerURL)

	return count, err
}

func (db *sqlDB) countDeviceWithID(deviceID string) (int, error) {
	var count int
	err := db.cli.Get(&count, `select count(*) from device WHERE device_id=?`, deviceID)

	return count, err
}

func (db *sqlDB) countCfgWith(areaID, schedulerURL string) (int, error) {
	var count int
	err := db.cli.Get(&count, `SELECT count(*) FROM config WHERE scheduler_url=? and area_id=?`, schedulerURL, areaID)

	return count, err
}
