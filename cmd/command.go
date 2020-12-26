package cmd

import (
	"github.com/gookit/config/v2"
	"github.com/gookit/config/v2/toml"
	"github.com/gookit/config/v2/yaml"
	"github.com/gookit/slog"
	"github.com/inherelab/genid/mysqlid"
)

const (
	logFileName = "genid.log"
	MaxLogSize  = 1024 * 1024 * 1024
)

func init() {
	slog.DefaultChannelName = "IDGenerator"
	config.AddDriver(toml.Driver)
	config.AddDriver(yaml.Driver)
}

func prepare(confFile string) error {
	// loac config
	slog.Info("load config from:", confFile)
	err := config.LoadFiles(confFile)
	if err != nil {
		return err
	}

	// init mysqlId generator
	dbCfg := &mysqlid.DBConfig{}
	err = config.MapStruct("db", dbCfg)
	if err != nil {
		return err
	}
	// dump.Println(dbCfg)

	_, err = mysqlid.InitSqlDB(dbCfg)

	return err
}
