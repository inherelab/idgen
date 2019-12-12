package server

import (
	"database/sql"
	"fmt"
	"sync"

	_ "github.com/go-sql-driver/mysql"
)

const (
	// create key table
	CreateTableSQLFormat = `
	CREATE TABLE %s (
    id bigint(20) unsigned NOT NULL auto_increment,
    PRIMARY KEY  (id)
) ENGINE=Innodb DEFAULT CHARSET=utf8 `

	// create key table if not exist
	CreateTableNTSQLFormat = `
	CREATE TABLE IF NOT EXISTS %s (
    id bigint(20) unsigned NOT NULL auto_increment,
    PRIMARY KEY  (id)
) ENGINE=Innodb DEFAULT CHARSET=utf8 `

	DropTableSQLFormat   = `DROP TABLE IF EXISTS %s`
	InsertIdSQLFormat    = "INSERT INTO %s(id) VALUES(%d)"
	SelectForUpdate      = "SELECT id FROM %s FOR UPDATE"
	UpdateIdSQLFormat    = "UPDATE %s SET id = id + %d"
	GetRowCountSQLFormat = "SELECT count(*) FROM %s"
	GetKeySQLFormat      = "show tables like '%s'"

	// Move to config
	// BatchCount = 3000
)

// MySQLIdGenerator struct definition
type MySQLIdGenerator struct {
	db       *sql.DB
	key      string // id generator key name
	cur      int64  // current id
	batchMax int64  // max id till get from mysql
	batch    int64  // get batch count ids from mysql once

	lock sync.Mutex
}

// NewMySQLIdGenerator create new MySQLIdGenerator
func NewMySQLIdGenerator(db *sql.DB, section string) (*MySQLIdGenerator, error) {
	idGenerator := new(MySQLIdGenerator)
	idGenerator.db = db
	if len(section) == 0 {
		return nil, fmt.Errorf("section is nil")
	}
	err := idGenerator.SetSection(section)
	if err != nil {
		return nil, err
	}

	idGenerator.batch = cfg.BatchCount
	idGenerator.cur = 0
	idGenerator.batchMax = idGenerator.cur
	return idGenerator, nil
}

// SetSection by key
func (m *MySQLIdGenerator) SetSection(key string) error {
	m.key = key
	return nil
}

// get id from key table
func (m *MySQLIdGenerator) getIdFromMySQL() (int64, error) {
	var id int64
	selectForUpdate := fmt.Sprintf(SelectForUpdate, getTableName(m.key))
	tx, err := m.db.Begin()
	if err != nil {
		return 0, err
	}

	rows, err := tx.Query(selectForUpdate)
	if err != nil {
		if err = tx.Rollback(); err != nil {
			return 0, err
		}
		return 0, err
	}

	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(&id)
		if err != nil {
			if err = tx.Rollback(); err != nil {
				return 0, err
			}
			return 0, err
		}
	}

	err = tx.Commit()
	return id, err
}

// Current get current id
func (m *MySQLIdGenerator) Current() (int64, error) {
	m.lock.Lock()
	defer m.lock.Unlock()

	return m.cur, nil
}

// Next get next Id
func (m *MySQLIdGenerator) Next() (int64, error) {
	var id int64
	var haveValue bool
	selectForUpdate := fmt.Sprintf(SelectForUpdate, getTableName(m.key))
	updateIdSql := fmt.Sprintf(UpdateIdSQLFormat, getTableName(m.key), m.batch)
	m.lock.Lock()
	defer m.lock.Unlock()

	if m.batchMax < m.cur+1 {
		tx, err := m.db.Begin()
		if err != nil {
			return 0, err
		}

		rows, err := tx.Query(selectForUpdate)
		if err != nil {
			if err = tx.Rollback(); err != nil {
				return 0, err
			}
			return 0, err
		}

		defer rows.Close()
		for rows.Next() {
			err := rows.Scan(&id)
			if err != nil {
				if err = tx.Rollback(); err != nil {
					return 0, err
				}
				return 0, err
			}
			haveValue = true
		}

		// When the idgo table has no id key
		if haveValue == false {
			return 0, fmt.Errorf("%s:have no id key", m.key)
		}
		_, err = tx.Exec(updateIdSql)
		if err != nil {
			if err = tx.Rollback(); err != nil {
				return 0, err
			}
			return 0, err
		}

		err = tx.Commit()
		if err != nil {
			return 0, err
		}

		// batchMax is larger than cur BatchCount
		m.batchMax = id + cfg.BatchCount
		m.cur = id
	}

	m.cur++
	return m.cur, nil
}

func (m *MySQLIdGenerator) Init() error {
	var err error

	m.lock.Lock()
	defer m.lock.Unlock()

	m.cur, err = m.getIdFromMySQL()
	if err != nil {
		return err
	}
	
	m.batchMax = m.cur
	return nil
}

// if force is true, create table directly
// if force is false, create table use CreateTableNTSQLFormat
func (m *MySQLIdGenerator) Reset(idOffset int64, force bool) (err error) {
	tableName := getTableName(m.key)

	createTableSQL := fmt.Sprintf(CreateTableSQLFormat, tableName)
	createTableNtSQL := fmt.Sprintf(CreateTableNTSQLFormat, tableName)
	dropTableSQL := fmt.Sprintf(DropTableSQLFormat, tableName)

	m.lock.Lock()
	defer m.lock.Unlock()

	if force == true {
		_, err = m.db.Exec(dropTableSQL)
		if err != nil {
			return
		}
		_, err = m.db.Exec(createTableSQL)
		if err != nil {
			return
		}
	} else {
		var rowCount int64
		_, err = m.db.Exec(createTableNtSQL)
		if err != nil {
			return
		}

		// check the idgo value if exist
		getRowCountSQL := fmt.Sprintf(GetRowCountSQLFormat, getTableName(m.key))
		rows, err := m.db.Query(getRowCountSQL)
		if err != nil {
			return err
		}

		defer rows.Close()
		for rows.Next() {
			err := rows.Scan(&rowCount)
			if err != nil {
				return err
			}
		}

		if rowCount == int64(1) {
			m.cur, err = m.getIdFromMySQL()
			if err != nil {
				return err
			}
			m.batchMax = m.cur
			return nil
		}
	}

	insertIdSQL := fmt.Sprintf(InsertIdSQLFormat, getTableName(m.key), idOffset)
	_, err = m.db.Exec(insertIdSQL)
	if err != nil {
		if _, err1 := m.db.Exec(dropTableSQL); err1 != nil {
			return err1
		}
		return err
	}

	m.cur = idOffset
	m.batchMax = m.cur
	return nil
}

// DelKeyTable delete key table
func (m *MySQLIdGenerator) DelKeyTable(key string) error {
	dropTableSQL := fmt.Sprintf(DropTableSQLFormat, getTableName(key))

	m.lock.Lock()
	defer m.lock.Unlock()

	_, err := m.db.Exec(dropTableSQL)
	return err
}

func getTableName(key string) string  {
	return cfg.TablePrefix + key
}
