package sqldb

import (
	"fmt"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

func NewDB(path string) (*sqlx.DB, error) {
	path = fmt.Sprintf("%s?parseTime=true&loc=Local", path)

	client, err := sqlx.Open("mysql", path)
	if err != nil {
		return nil, err
	}

	if err = client.Ping(); err != nil {
		return nil, err
	}

	return client, nil
}
