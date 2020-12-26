package genid

import (
	"database/sql"
	"fmt"
	"io/ioutil"

	"github.com/BurntSushi/toml"
	"github.com/gookit/slog"
)

// Config struct
type Config struct {
	// the server listen addr
	Addr        string `toml:"addr"`
	LogPath     string `toml:"log_path"`
	LogLevel    string `toml:"log_level"`
	BatchCount  int64  `toml:"batch_count"`
	TableMode   string `toml:"table_mode"`
	TableName   string `toml:"table_name"`
	TablePrefix string `toml:"table_prefix"`
	// db config
	DbConfig *DBConfig `toml:"db"`
}

// DBConfig mysql db config struct
type DBConfig struct {
	Host         string `toml:"host"`
	Port         int    `toml:"port"`
	User         string `toml:"user"`
	Password     string `toml:"password"`
	DBName       string `toml:"db_name"`
	// TODO db pool config
	MaxIdleConns int    `toml:"max_idle_conns"`
}

var cfg = &Config{
	TableName:   "gid_keys",
	TablePrefix: "gid_key_",
}

// CreateSqlDB
func (c *Config) CreateSqlDB() (*sql.DB, error) {
	// init db
	proto := "mysql"
	charset := "utf8"

	// eg: "root:@tcp(127.0.0.1:3306)/test?charset=utf8"
	cfg := c.DbConfig
	url := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?%s",
		cfg.User,
		cfg.Password,
		cfg.Host,
		cfg.Port,
		cfg.DBName,
		charset,
	)

	slog.Infof("init mysqlId DB connection, host:%s db:%s", cfg.Host, cfg.DBName)

	return sql.Open(proto, url)
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
