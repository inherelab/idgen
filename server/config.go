package server

import (
	"io/ioutil"

	"github.com/BurntSushi/toml"
)

// Config struct
type Config struct {
	Addr     string    `toml:"addr"`
	LogPath  string    `toml:"log_path"`
	LogLevel string    `toml:"log_level"`
	DbConfig *DBConfig `toml:"storage_db"`
	TableName string 	`toml:"table_name"`
}

// DBConfig mysql db config struct
type DBConfig struct {
	Host         string `toml:"host"`
	Port         int    `toml:"port"`
	User         string `toml:"user"`
	Password     string `toml:"password"`
	DBName       string `toml:"db_name"`
	MaxIdleConns int    `toml:"max_idle_conns"`
}

// ParseConfigFile parse config file
func ParseConfigFile(fileName string) (*Config, error) {
	var cfg Config

	data, err := ioutil.ReadFile(fileName)
	if err != nil {
		return nil, err
	}

	_, err = toml.Decode(string(data), &cfg)
	if err != nil {
		return nil, err
	}
	return &cfg, nil
}
