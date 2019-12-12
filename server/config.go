package server

import (
	"io/ioutil"

	"github.com/BurntSushi/toml"
)

// Config struct
type Config struct {
	Addr        string    `toml:"addr"`
	LogPath     string    `toml:"log_path"`
	LogLevel    string    `toml:"log_level"`
	DbConfig    *DBConfig `toml:"storage_db"`
	BatchCount  int64       `toml:"batch_count"`
	TableName   string    `toml:"table_name"`
	TablePrefix string    `toml:"table_prefix"`
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

var cfg = &Config{
	TableName:   "gid_keys",
	TablePrefix: "gid_key_",
}

// SetConfig set config
func SetConfig(c *Config) {
	cfg = c
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
