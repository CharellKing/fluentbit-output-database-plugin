package main

import (
	"context"
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"strings"
)

const (
	DefaultDialect   = "mysql"
	DefaultUser      = "root"
	DefaultPassword  = ""
	DefaultAddress   = "127.0.0.1:3306"
	DefaultDatabase  = "test_db"
	DefaultTable     = "test"
	DefaultBatchSize = 100
)

type DatabasePluginConfig struct {
	PluginInstanceID int

	Dialect  string
	User     string
	Password string
	Address  string
	Database string
	Table    string

	BatchSize int
}

func (c *DatabasePluginConfig) GetDsn() string {
	return fmt.Sprintf("%s:%s@tcp(%s)/%s", c.User, c.Password, c.Address, c.Database)
}

type DatabasePlugin struct {
	SugarLogger      *zap.SugaredLogger
	PluginInstanceId int

	Conn      *sql.DB
	Table     string
	BatchSize int
	SQL       string

	Columns []string
}

func NewDatabasePlugin(c *DatabasePluginConfig) (*DatabasePlugin, error) {
	conn, err := sql.Open(c.Dialect, c.GetDsn())
	if err != nil {
		return nil, err
	}

	if err := conn.Ping(); err != nil {
		return nil, errors.WithStack(err)
	}

	columns, err := getFields(conn, c.Table)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	placeholders := make([]string, len(columns))
	for i := range placeholders {
		placeholders[i] = "?"
	}

	sqlTpl := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", c.Table, strings.Join(columns, ","),
		strings.Join(placeholders, ","))

	sugarLogger.Infof("Plugin %d: SQL: %s", c.PluginInstanceID, sqlTpl)

	return &DatabasePlugin{
		SugarLogger:      sugarLogger,
		PluginInstanceId: c.PluginInstanceID,
		Conn:             conn,
		Table:            c.Table,
		BatchSize:        c.BatchSize,
		Columns:          columns,
		SQL:              sqlTpl,
	}, nil
}

func getFields(conn *sql.DB, table string) ([]string, error) {
	query := fmt.Sprintf("SHOW COLUMNS FROM %s", table)
	rows, err := conn.Query(query)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	defer func() {
		_ = rows.Close()
	}()

	var columns []string
	for rows.Next() {
		var field, colType, null, key, defaultValue, extra string
		err := rows.Scan(&field, &colType, &null, &key, &defaultValue, &extra)
		if err != nil {
			return nil, errors.WithStack(err)
		}

		columns = append(columns, field)
	}

	return columns, nil
}

func (p *DatabasePlugin) BatchWrite(records []map[interface{}]interface{}) error {
	ctx := context.Background()
	if len(records) <= 0 {
		return nil
	}

	tx, err := p.Conn.Begin()
	if err != nil {
		return errors.WithStack(err)
	}

	stmt, err := tx.PrepareContext(ctx, p.SQL)
	if err != nil {
		return errors.WithStack(err)
	}
	defer func() {
		_ = stmt.Close()
	}()

	for _, record := range records {
		values := make([]interface{}, len(p.Columns))
		for i, col := range p.Columns {
			values[i] = record[col]
		}
		_, err := stmt.ExecContext(ctx, values...)
		if err != nil {
			if err := tx.Rollback(); err != nil {
				return errors.WithStack(err)
			}
			return errors.WithStack(err)
		}
	}

	// commit and record metrics
	if err = tx.Commit(); err != nil {
		return errors.WithStack(err)
	}

	return nil
}
