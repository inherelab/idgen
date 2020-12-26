package mysqlid

import (
	"database/sql"
	"fmt"

	"github.com/gookit/slog"
)

var Db *sql.DB
var cfg = &Config{
	TableName: "__idgen_manager",
}

// Config struct
type Config struct {
	// the server listen addr
	Addr     string `toml:"addr"`
	LogPath  string `toml:"log_path"`
	LogLevel string `toml:"log_level"`

	BatchCount  int64  `toml:"batch_count"`
	TableMode   string `toml:"table_mode"`
	TableName   string `toml:"table_name"`
	TablePrefix string `toml:"table_prefix"`

	// db config
	DbConfig *DBConfig `toml:"db"`
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

// DBConfig struct
type DBConfig struct {
	Host         string `mapstructure:"host" yaml:"host"`
	Port         int    `mapstructure:"port" yaml:"port"`
	User         string `mapstructure:"user" yaml:"user"`
	Password     string `mapstructure:"password" yaml:"password"`
	DBName       string `mapstructure:"db_name" yaml:"db_name"`
	MaxIdleConns int    `mapstructure:"max_idle_conns" yaml:"max_idle_conns"`
}

func InitSqlDB(cfg *DBConfig) (*sql.DB, error) {
	var err error
	// init db
	proto := "mysql"
	charset := "utf8"
	// root:@tcp(127.0.0.1:3306)/test?charset=utf8
	url := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?%s",
		cfg.User,
		cfg.Password,
		cfg.Host,
		cfg.Port,
		cfg.DBName,
		charset,
	)

	slog.Infof("init mysqlId DB connection, host:%s db:%s", cfg.Host, cfg.DBName)

	Db, err = sql.Open(proto, url)
	return Db, err
}
