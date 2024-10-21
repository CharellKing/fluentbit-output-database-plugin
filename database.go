package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/pkg/errors"
	"github.com/samber/lo"
	"github.com/spf13/cast"
	"go.uber.org/zap"
	"reflect"
	"regexp"
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

	BatchSize     int
	IgnoreColumns []string
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

	Columns       []string
	ColumnMap     map[string]string
	IgnoreColumns []string
}

func NewDatabasePlugin(sugaredLogger *zap.SugaredLogger, c *DatabasePluginConfig) (*DatabasePlugin, error) {
	conn, err := sql.Open(c.Dialect, c.GetDsn())
	if err != nil {
		return nil, err
	}

	if err := conn.Ping(); err != nil {
		return nil, errors.WithStack(err)
	}

	columns, columnMap, err := getFields(conn, c.Table, c.IgnoreColumns)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	placeholders := make([]string, len(columns))
	for i := range placeholders {
		placeholders[i] = "?"
	}

	sqlTpl := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", c.Table, strings.Join(columns, ","),
		strings.Join(placeholders, ","))

	sugaredLogger.Infof("Plugin %d: SQL: %s", c.PluginInstanceID, sqlTpl)

	return &DatabasePlugin{
		SugarLogger:      sugaredLogger,
		PluginInstanceId: c.PluginInstanceID,
		Conn:             conn,
		Table:            c.Table,
		BatchSize:        c.BatchSize,
		Columns:          columns,
		ColumnMap:        columnMap,
		SQL:              sqlTpl,
		IgnoreColumns:    c.IgnoreColumns,
	}, nil
}

func getFields(conn *sql.DB, table string, ignoreColumns []string) ([]string, map[string]string, error) {
	query := fmt.Sprintf("SHOW COLUMNS FROM %s", table)
	rows, err := conn.Query(query)
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}
	defer func() {
		_ = rows.Close()
	}()

	var columns []string
	columnMap := make(map[string]string)
	re := regexp.MustCompile(`\w+`)

	for rows.Next() {
		var (
			field, colType, null, key, extra string
			defaultValue                     interface{}
		)
		err := rows.Scan(&field, &colType, &null, &key, &defaultValue, &extra)
		if err != nil {
			return nil, nil, errors.WithStack(err)
		}

		if lo.Contains(ignoreColumns, field) {
			continue
		}

		columns = append(columns, cast.ToString(field))
		columnMap[field] = re.FindString(colType)
	}

	return columns, columnMap, nil
}

func (p *DatabasePlugin) convertFieldValue(fieldType string, value interface{}) interface{} {
	switch fieldType {
	case "varchar", "tinytext", "mediumtext", "longtext", "text", "tinyblob", "mediumblob", "longblob", "blob":
		v := reflect.ValueOf(value)
		if v.Kind() == reflect.Slice || v.Kind() == reflect.Map {
			bytesValue, _ := json.Marshal(value)
			value = string(bytesValue)
		}
	}
	return value
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

	if len(records) > 0 {
		recordBytes, _ := json.MarshalIndent(records[0], "", "  ")
		p.SugarLogger.Infof("record: %s", string(recordBytes))
	}

	for _, record := range records {
		values := make([]interface{}, len(p.Columns))
		for i, col := range p.Columns {
			values[i] = p.convertFieldValue(p.ColumnMap[col], record[col])
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
