package mysqlid

import (
	"database/sql"
	"fmt"
	"sync"

	_ "github.com/go-sql-driver/mysql"
	"github.com/gookit/slog"
)

const (
	// create name table
	CreateTableSQLFormat = `
CREATE TABLE %s (
    id bigint(20) unsigned NOT NULL auto_increment,
    PRIMARY KEY  (id)
) ENGINE=Innodb DEFAULT CHARSET=utf8`

	// create name table if not exist
	CreateTableNTSQLFormat = `
CREATE TABLE IF NOT EXISTS %s (
    id bigint(20) unsigned NOT NULL auto_increment,
    PRIMARY KEY  (id)
) ENGINE=Innodb DEFAULT CHARSET=utf8`

	DropTableSQLFormat   = `DROP TABLE IF EXISTS %s`
	InsertIdSQLFormat    = "INSERT INTO %s(`id`) VALUES(%d)"
	SelectForUpdate      = "SELECT `id` FROM %s FOR UPDATE"
	UpdateIdSQLFormat    = "UPDATE `%s` SET `id` = `id` + %d"
	GetRowCountSQLFormat = "SELECT count(*) FROM `%s`"
	// SHOW TABLES LIKE '%service_user%';
	// SHOW TABLES WHERE Tables_in_{DB_NAME} = 'service_user';
	GetKeySQLFormat      = "SHOW TABLES LIKE '%s'"

	// 获取id的自增步长
	BatchCount = 2000
)

// Generator struct
type Generator struct {
	db   *sql.DB

	// the service name. id generator name name.
	name string
	lock sync.Mutex

	current int64 // current id
	batchMax int64  // max id till get from mysql
	batch    int64  // get batch count ids from mysql once
}

func NewGenerator(db *sql.DB, serviceName string) (*Generator, error) {
	if len(serviceName) == 0 {
		return nil, fmt.Errorf("service name is nil")
	}

	generator := new(Generator)
	generator.db = db

	// err := generator.SetSection(serviceName)
	// if err != nil {
	// 	return nil, err
	// }

	generator.name = serviceName

	generator.current = 0
	generator.batch = BatchCount
	// generator.batchMax = BatchCount
	generator.batchMax = 0

	return generator, nil
}

// Init the generator
func (m *Generator) Init() error {
	var err error

	m.lock.Lock()
	defer m.lock.Unlock()

	m.current, err = m.getLastIdFromDb()
	if err != nil {
		return err
	}
	m.batchMax = m.current

	return nil
}

// func (m *Generator) SetSection(name string) error {
// 	m.name = name
// 	return nil
// }

// get last id from db table
func (m *Generator) getLastIdFromDb() (int64, error) {
	selectForUpdate := fmt.Sprintf(SelectForUpdate, m.name)
	tx, err := m.db.Begin()
	if err != nil {
		return 0, err
	}

	slog.Infof("SQL=%s", selectForUpdate)
	rows, err := tx.Query(selectForUpdate)
	if err != nil {
		tx.Rollback()
		return 0, err
	}

	var id int64
	defer rows.Close()

	// read one line
	for rows.Next() {
		err := rows.Scan(&id)
		if err != nil {
			tx.Rollback()
			return 0, err
		}
	}

	tx.Commit()

	slog.Infof("get last id form db table: %s, last: %d", m.name, id)
	return id, nil
}

// Current get current id
func (m *Generator) Current() int64 {
	m.lock.Lock()
	defer m.lock.Unlock()

	return m.current
}

// Next get next id
func (m *Generator) Next() (int64, error) {
	var id int64
	var haveValue bool

	selectForUpdate := fmt.Sprintf(SelectForUpdate, m.name)
	updateIdSql := fmt.Sprintf(UpdateIdSQLFormat, m.name, m.batch)

	m.lock.Lock()
	defer m.lock.Unlock()

	if m.batchMax < m.current+1 {
		tx, err := m.db.Begin()
		if err != nil {
			return 0, err
		}

		slog.Infof("max=%d cur=%d SQL=%s", m.batchMax, m.current, selectForUpdate)
		rows, err := tx.Query(selectForUpdate)
		if err != nil {
			tx.Rollback()
			return 0, err
		}

		defer rows.Close()
		for rows.Next() {
			err := rows.Scan(&id)
			if err != nil {
				tx.Rollback()
				return 0, err
			}
			haveValue = true
		}

		// When the table has no id name
		if haveValue == false {
			return 0, fmt.Errorf("%s: have no id name", m.name)
		}

		slog.Infof("dbId=%d SQL=%s", id, updateIdSql)
		_, err = tx.Exec(updateIdSql)
		if err != nil {
			tx.Rollback()
			return 0, err
		}

		tx.Commit()

		// batchMax is larger than current BatchCount
		m.batchMax = id + BatchCount
		m.current = id
	}

	m.current++
	return m.current, nil
}

// if force is true, create table directly
// if force is false, create table use CreateTableNTSQLFormat
func (m *Generator) Reset(idOffset int64, force bool) error {
	var err error
	createTableSQL := fmt.Sprintf(CreateTableSQLFormat, m.name)
	createTableNtSQL := fmt.Sprintf(CreateTableNTSQLFormat, m.name)
	dropTableSQL := fmt.Sprintf(DropTableSQLFormat, m.name)

	m.lock.Lock()
	defer m.lock.Unlock()

	// drop table an create table
	if force == true {
		slog.Infof("SQL=%s", dropTableSQL)
		_, err = m.db.Exec(dropTableSQL)
		if err != nil {
			return err
		}

		slog.Infof("SQL=%s", createTableSQL)
		_, err = m.db.Exec(createTableSQL)
		if err != nil {
			return err
		}
	} else {
		var rowCount int64
		slog.Infof("SQL=%s", createTableNtSQL)
		_, err = m.db.Exec(createTableNtSQL)
		if err != nil {
			return err
		}

		// check the value if exist
		getRowCountSQL := fmt.Sprintf(GetRowCountSQLFormat, m.name)

		slog.Infof("SQL=%s", getRowCountSQL)
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
			break
		}

		// has record. update id to latest.
		// NOTICE: dont update db id value to `idOffset`.
		if rowCount == int64(1) {
			m.current, err = m.getLastIdFromDb()
			if err != nil {
				return err
			}

			m.batchMax = m.current
			return nil
		}
	}

	insertIdSQL := fmt.Sprintf(InsertIdSQLFormat, m.name, idOffset)
	slog.Infof("SQL=%s", insertIdSQL)
	_, err = m.db.Exec(insertIdSQL)
	if err != nil {
		m.db.Exec(dropTableSQL)
		return err
	}

	m.current = idOffset
	m.batchMax = m.current
	return nil
}

func (m *Generator) DelKeyTable(key string) error {
	dropTableSQL := fmt.Sprintf(DropTableSQLFormat, key)

	m.lock.Lock()
	defer m.lock.Unlock()

	slog.Infof("SQL=%s", dropTableSQL)
	_, err := m.db.Exec(dropTableSQL)
	if err != nil {
		return err
	}
	return nil
}
