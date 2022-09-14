package persistent

import (
	"github.com/jmoiron/sqlx"
)

// TypeSQL Sql
func TypeSQL() string {
	return "Sql"
}

type sqlDB struct {
	cli *sqlx.DB
}

// InitSQL init sql
func InitSQL(url string) (DB, error) {
	// fmt.Printf("redis init url:%v", url)

	db := &sqlDB{}
	// sqlxdb, err := sqlx.Connect("postgres", "user=foo dbname=bar sslmode=disable")
	database, err := sqlx.Open("mysql", "root:123456@tcp(127.0.0.1:3306)/test")
	if err != nil {
		return nil, err
	}

	if err := database.Ping(); err != nil {
		return nil, err
	}

	db.cli = database

	return db, nil
}

// IsNilErr Is NilErr
func (sd sqlDB) IsNilErr(err error) bool {
	return false
}
