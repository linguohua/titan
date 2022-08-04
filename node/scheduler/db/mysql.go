package db

import (
	"encoding/json"
	"github.com/linguohua/titan/api"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"io/ioutil"
	"log"
)

var (
	GMysqlDb *gorm.DB
	GvaMysql GeneralDB
	GConfig  map[string]interface{}
)

const sys = "system"

// GeneralDB 也被 Pgsql 和 Mysql 原样使用
type GeneralDB struct {
	Path         string `mapstructure:"path" json:"path" yaml:"path"`                               // 服务器地址:端口
	Port         string `mapstructure:"port" json:"port" yaml:"port"`                               //:端口
	Config       string `mapstructure:"config" json:"config" yaml:"config"`                         // 高级配置
	Dbname       string `mapstructure:"db-name" json:"db-name" yaml:"db-name"`                      // 数据库名
	Username     string `mapstructure:"username" json:"username" yaml:"username"`                   // 数据库用户名
	Password     string `mapstructure:"password" json:"password" yaml:"password"`                   // 数据库密码
	MaxIdleConns int    `mapstructure:"max-idle-conns" json:"max-idle-conns" yaml:"max-idle-conns"` // 空闲中的最大连接数
	MaxOpenConns int    `mapstructure:"max-open-conns" json:"max-open-conns" yaml:"max-open-conns"` // 打开到数据库的最大连接数
	LogMode      string `mapstructure:"log-mode" json:"log-mode" yaml:"log-mode"`                   // 是否开启Gorm全局日志
	LogZap       bool   `mapstructure:"log-zap" json:"log-zap" yaml:"log-zap"`                      // 是否通过zap写入日志文件
}
type Mysql struct {
	GeneralDB `yaml:",inline" mapstructure:",squash"`
}

func (m *GeneralDB) Dsn() string {
	return m.Username + ":" + m.Password + "@tcp(" + m.Path + ":" + m.Port + ")/" + m.Dbname + "?" + m.Config
}

func (m *GeneralDB) GetLogMode() string {
	return m.LogMode
}
func ConfigInit() {
	config, err := loadConfig("../../node/scheduler/db/config.json")
	if err != nil {
		log.Fatal("fatal error01: ", err.Error())
	}
	GConfig = config
}
func loadConfig(path string) (map[string]interface{}, error) {

	file, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var config map[string]interface{}
	if err = json.Unmarshal(file, &config); err != nil {
		return nil, err
	}
	var sqlPath GeneralDB
	if err = json.Unmarshal(file, &sqlPath); err != nil {
		return nil, err
	}
	GvaMysql = sqlPath
	return config, nil
}

func CreateTables() error {
	M := GMysqlDb.Migrator()
	if M.HasTable(&api.HourDataOfDaily{}) {
		print("HourDataOfDaily:已经存在")
	} else {
		// 不存在就创建表
		err := GMysqlDb.AutoMigrate(&api.HourDataOfDaily{})
		if err != nil {
			return err
		}
	}
	if M.HasTable(&api.IncomeDaily{}) {
		print("IncomeDaily:已经存在")
	} else {
		// 不存在就创建表
		err := GMysqlDb.AutoMigrate(&api.IncomeDaily{})
		if err != nil {
			return err
		}
	}
	return nil
}

func GormMysql() *gorm.DB {
	ConfigInit()
	m := GvaMysql
	if m.Dbname == "" {
		return nil
	}
	mysqlConfig := mysql.Config{
		DSN:                       m.Dsn(), // DSN data source name
		DefaultStringSize:         191,     // string 类型字段的默认长度
		SkipInitializeWithVersion: false,   // 根据版本自动配置
	}
	if db, err := gorm.Open(mysql.New(mysqlConfig), nil); err != nil {
		return nil
	} else {
		sqlDB, _ := db.DB()
		sqlDB.SetMaxIdleConns(m.MaxIdleConns)
		sqlDB.SetMaxOpenConns(m.MaxOpenConns)
		return db
	}

}
