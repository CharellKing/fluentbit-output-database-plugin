package main

import (
	"C"
	"strings"
	"time"

	"fmt"
	"unsafe"

	"github.com/fluent/fluent-bit-go/output"
	"github.com/spf13/cast"
	"go.uber.org/zap"
)

var (
	pluginName = "database"
	version    = "v1.0.0"

	sugarLogger     *zap.SugaredLogger
	pluginInstances []*DatabasePlugin
)

func initLogger() {
	logger, _ := zap.NewProduction()
	sugarLogger = logger.Sugar()
}

func addPluginInstance(ctx unsafe.Pointer) error {
	pluginID := len(pluginInstances)

	config := getConfiguration(ctx, pluginID)

	instance, err := NewDatabasePlugin(sugarLogger, config)
	if err != nil {
		return err
	}

	output.FLBPluginSetContext(ctx, pluginID)
	pluginInstances = append(pluginInstances, instance)

	return nil
}

func getPluginInstance(ctx unsafe.Pointer) *DatabasePlugin {
	pluginID := output.FLBPluginGetContext(ctx).(int)
	return pluginInstances[pluginID]
}

func getConfiguration(ctx unsafe.Pointer, pluginID int) *DatabasePluginConfig {
	var config DatabasePluginConfig
	config.PluginInstanceID = pluginID

	config.Dialect = output.FLBPluginConfigKey(ctx, "dialect")
	if config.Dialect == "" {
		config.Dialect = DefaultDialect
	}

	config.User = output.FLBPluginConfigKey(ctx, "user")
	if config.User == "" {
		config.User = DefaultUser
	}

	config.Password = output.FLBPluginConfigKey(ctx, "password")
	if config.Password == "" {
		config.Password = DefaultPassword
	}

	config.Address = output.FLBPluginConfigKey(ctx, "address")
	if config.Address == "" {
		config.Address = DefaultAddress
	}

	config.Database = output.FLBPluginConfigKey(ctx, "database")
	if config.Database == "" {
		config.Database = DefaultDatabase
	}

	config.Table = output.FLBPluginConfigKey(ctx, "table")
	if config.Table == "" {
		config.Table = DefaultTable
	}

	ignoreColumnStr := output.FLBPluginConfigKey(ctx, "ignoreColumns")
	if ignoreColumnStr != "" {
		config.IgnoreColumns = strings.Fields(ignoreColumnStr)
	}

	batchSize := output.FLBPluginConfigKey(ctx, "batchSize")
	if batchSize == "" {
		config.BatchSize = DefaultBatchSize
	} else {
		config.BatchSize = cast.ToInt(batchSize)
	}

	return &config
}

//export FLBPluginRegister
func FLBPluginRegister(def unsafe.Pointer) int {
	// Gets called only once when the plugin.so is loaded
	initLogger()
	return output.FLBPluginRegister(def, pluginName, fmt.Sprintf("%s output plugin %s", pluginName, version))
}

//export FLBPluginInit
func FLBPluginInit(ctx unsafe.Pointer) int {
	// Gets called only once for each instance you have configured.
	err := addPluginInstance(ctx)
	if err != nil {
		sugarLogger.Error(err)
		return output.FLB_ERROR
	}
	return output.FLB_OK
}

//export FLBPluginFlushCtx
func FLBPluginFlushCtx(ctx, data unsafe.Pointer, length C.int, tag *C.char) int {
	var count int
	var ret int
	var record map[interface{}]interface{}

	// Create Fluent Bit decoder
	dec := output.NewDecoder(data, int(length))

	databasePlugin := getPluginInstance(ctx)

	fluentTag := C.GoString(tag)

	sugarLogger.Debug("flush data", zap.Int("pluginInstanceId", databasePlugin.PluginInstanceId),
		zap.String("tag", fluentTag))

	var records []map[interface{}]interface{}
	writeDuration := 1 * time.Minute
	lastWriteTime := time.Now()

	for {
		// Extract Record
		ret, _, record = output.GetRecord(dec)
		if ret != 0 {
			break
		}

		records = append(records, record)

		if len(records) >= databasePlugin.BatchSize || time.Since(lastWriteTime) >= writeDuration {
			err := databasePlugin.BatchWrite(records)
			if err != nil {
				sugarLogger.Error("batch write failed",
					zap.Int("pluginInstanceId", databasePlugin.PluginInstanceId),
					zap.String("tag", fluentTag), zap.Error(err))
			}
			records = []map[interface{}]interface{}{}
			lastWriteTime = time.Now()
		}
		count++
	}

	if len(records) > 0 {
		err := databasePlugin.BatchWrite(records)
		if err != nil {
			sugarLogger.Error("batch write failed",
				zap.Int("pluginInstanceId", databasePlugin.PluginInstanceId),
				zap.String("tag", fluentTag), zap.Error(err))
		}
	}

	sugarLogger.Info("process events", zap.Int("pluginInstanceId", databasePlugin.PluginInstanceId),
		zap.String("tag", fluentTag),
		zap.Int("eventsCount", count))
	return output.FLB_OK
}

//export FLBPluginExit
func FLBPluginExit() int {
	return output.FLB_OK
}

func main() {
}
