package main

import (
	"encoding/json"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"io"
	"os"
	"testing"
)

func getDatabasePlugin(table string, ignoreColumns []string) (*DatabasePlugin, error) {
	cfg := DatabasePluginConfig{
		1,
		"mysql",
		"root",
		"123456",
		"127.0.0.1:3306",
		"fluentbit",
		table,
		100,
		ignoreColumns,
	}

	logger, _ := zap.NewProduction()
	sugarLogger := logger.Sugar()

	plugin, err := NewDatabasePlugin(sugarLogger, &cfg)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return plugin, nil
}

func getFileContent(filePath string) (map[interface{}]interface{}, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	defer func() {
		_ = file.Close()
	}()

	dataBytes, err := io.ReadAll(file)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	data := make(map[string]interface{})
	err = json.Unmarshal(dataBytes, &data)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	dataRecord := make(map[interface{}]interface{})
	for key, value := range data {
		dataRecord[key] = value
	}
	return dataRecord, nil
}

func TestInsertMetrics(t *testing.T) {
	databasePlugin, err := getDatabasePlugin("samples", []string{"updated"})
	if err != nil {
		t.Fatal(err)
	}
	record, err := getFileContent("D:\\code\\fluentbit-output-database-plugin\\data\\metric.json")
	if err != nil {
		t.Fatal(err)
	}
	err = databasePlugin.BatchWrite([]map[interface{}]interface{}{
		record,
	})
	if err != nil {
		t.Fatal(err)
	}

}

func TestInsertBusiness(t *testing.T) {
	databasePlugin, err := getDatabasePlugin("sgc_trade_bussines_log", []string{"updated"})
	if err != nil {
		t.Fatal(err)
	}
	record, err := getFileContent("D:\\code\\fluentbit-output-database-plugin\\data\\business.json")
	if err != nil {
		t.Fatal(err)
	}
	err = databasePlugin.BatchWrite([]map[interface{}]interface{}{
		record,
	})
	if err != nil {
		t.Fatal(err)
	}

}

func TestInsertQuote(t *testing.T) {
	databasePlugin, err := getDatabasePlugin("symbol_quote_log", []string{"updated_at", "created_at", "deleted_at"})
	if err != nil {
		t.Fatal(err)
	}
	record, err := getFileContent("D:\\code\\fluentbit-output-database-plugin\\data\\quote.json")
	if err != nil {
		t.Fatal(err)
	}
	err = databasePlugin.BatchWrite([]map[interface{}]interface{}{
		record,
	})
	if err != nil {
		t.Fatal(err)
	}

}

func TestConvertBytesToString(t *testing.T) {
	databasePlugin, err := getDatabasePlugin("sgc_trade_bussines_log", []string{"updated"})
	if err != nil {
		t.Fatal(err)
	}

	d := databasePlugin.convertBytesToString([][]byte{
		[]byte("abc"),
		[]byte("def"),
		[]byte("ghi"),
	})

	t.Log(d)

	d = databasePlugin.convertBytesToString(map[string]interface{}{
		"a": [][]byte{
			[]byte("abc"),
			[]byte("def"),
			[]byte("ghi"),
		},
		"b": 1,
		"c": 12.3,
		"d": map[string]interface{}{
			"e": "abc",
			"f": "def",
			"g": []byte("abcdef"),
			"h": map[string]interface{}{
				"dehi": "12341242",
			},
		},
	})
	t.Log(d)
}
