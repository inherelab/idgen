package mysqlid

const (
	// CreateSingleTableSQL "IF NOT EXISTS"
	CreateSingleTableSQL = `CREATE TABLE %s (
    k VARCHAR(128) NOT NULL COMMENT "service name",
    id bigint(20) unsigned NOT NULL "global id for service",
    PRIMARY KEY (k)
) ENGINE=Innodb DEFAULT CHARSET=utf8`
)
