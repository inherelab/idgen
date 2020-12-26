package mysqlid

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/gookit/slog"
)

const (
	ManagerTableName = "_idgen_manager"

	CreateRecordTableSQLFormat = `
CREATE TABLE %s (
	k VARCHAR(255) NOT NULL COMMENT 'service name',
	PRIMARY KEY (k)
) ENGINE=Innodb DEFAULT CHARSET=utf8`

	// create name table if not exist
	CreateRecordTableNTSQLFormat = `
CREATE TABLE IF NOT EXISTS %s (
	k VARCHAR(255) NOT NULL COMMENT 'service name',
	PRIMARY KEY (k)
) ENGINE=Innodb DEFAULT CHARSET=utf8`

	InsertKeySQLFormat  = "INSERT INTO %s (`k`) VALUES ('%s')"
	SelectKeySQLFormat  = "SELECT `k` FROM `%s` WHERE `k` = '%s'"
	SelectKeysSQLFormat = "SELECT `k` FROM `%s`"
	DeleteKeySQLFormat  = "DELETE FROM `%s` WHERE `k` = '%s'"
)

var (
	ErrServiceNotExists = errors.New("service not exists")
)

// Manager struct
type Manager struct {
	sync.RWMutex
	db *sql.DB

	initialized bool
	generatorMap map[string]*Generator
}

// NewEmptyManager instance
func NewEmptyManager() *Manager {
	return &Manager{
		generatorMap: make(map[string]*Generator),
	}
}

// NewManager instance
func NewManager(db *sql.DB) *Manager {
	return &Manager{
		db: db,
		// init map
		generatorMap: make(map[string]*Generator),
	}
}

// Init all services info from DB
func (s *Manager) Init() error {
	// if has been initialized
	if s.initialized {
		return nil
	}

	slog.Info("init load all services info from DB")
	createTableNtSQL := fmt.Sprintf(CreateRecordTableNTSQLFormat, ManagerTableName)
	selectKeysSQL := fmt.Sprintf(SelectKeysSQLFormat, ManagerTableName)

	slog.Infof("SQL=%s", createTableNtSQL)
	_, err := s.db.Exec(createTableNtSQL)
	if err != nil {
		return err
	}

	slog.Infof("SQL=%s", selectKeysSQL)
	rows, err := s.db.Query(selectKeysSQL)
	if err != nil {
		return err
	}

	defer rows.Close()
	for rows.Next() {
		serviceName := ""
		err := rows.Scan(&serviceName)
		if err != nil {
			return err
		}

		if serviceName == "" {
			continue
		}

		gen, ok := s.generatorMap[serviceName]
		if ok == false {
			isExist, err := s.TableExist(serviceName)
			if err != nil {
				return err
			}

			if isExist {
				gen, err = NewGenerator(s.db, serviceName)
				if err != nil {
					return err
				}

				// TODO should init ?
				if err = gen.Init(); err != nil {
					return err
				}

				// storage
				s.generatorMap[serviceName] = gen
			}
		}
	}

	s.initialized = true
	return nil
}

// TableExist
// func (s *Manager) IsKeyExist(key string) (bool, error) {
func (s *Manager) TableExist(key string) (bool, error) {
	var tableName string
	var haveValue bool

	if len(key) == 0 {
		return false, nil
	}

	getKeySQL := fmt.Sprintf(GetKeySQLFormat, key)
	slog.Infof("SQL=%s", getKeySQL)
	rows, err := s.db.Query(getKeySQL)
	if err != nil {
		return false, err
	}

	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(&tableName)
		if err != nil {
			return false, err
		}
		haveValue = true
	}

	if haveValue == false {
		return false, nil
	}
	return true, nil
}

// GetKey This is mainly used to confirm the existence of the service name in the DB
// 这里主要用于确认DB中服务名存在
func (s *Manager) GetKey(key string) (string, error) {
	keyName := ""
	selectKeySQL := fmt.Sprintf(SelectKeySQLFormat, ManagerTableName, key)

	slog.Infof("SQL=%s", selectKeySQL)
	rows, err := s.db.Query(selectKeySQL)
	if err != nil {
		return keyName, err
	}

	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(&keyName)
		if err != nil {
			return keyName, err
		}
	}

	if keyName == "" {
		return keyName, fmt.Errorf("%s: not exists name", key)
	}
	return keyName, nil
}

func (s *Manager) SetKey(key string) error {
	if len(key) == 0 {
		return fmt.Errorf("%s: invalid name", key)
	}

	// service name record exists on manager table.
	_, err := s.GetKey(key)
	if err == nil {
		  return nil
	}
	// if err != nil {
	// 	return err
	// }

	insertKeySQL := fmt.Sprintf(InsertKeySQLFormat, ManagerTableName, key)
	slog.Infof("SQL=%s", insertKeySQL)
	_, err = s.db.Exec(insertKeySQL)

	return err
}

func (s *Manager) DelKey(key string) error {
	if len(key) == 0 {
		return fmt.Errorf("%s: invalid name", key)
	}

	_, err := s.GetKey(key)
	if err == nil {
		deleteKeySQL := fmt.Sprintf(DeleteKeySQLFormat, ManagerTableName, key)
		slog.Infof("SQL=%s", deleteKeySQL)
		_, err = s.db.Exec(deleteKeySQL)
		if err != nil {
			return err
		}
	}

	return nil
}

// GetGenerator by service name
func (s *Manager) GetGenerator(serviceName string) (*Generator, error) {
	s.Lock()
	gen, ok := s.generatorMap[serviceName]
	if ok == false {
		s.Unlock()
		return nil, ErrServiceNotExists
	}
	s.Unlock()

	return gen, nil
}

// GetOrNewGenerator by service name
func (s *Manager) GetOrNewGenerator(serviceName string) (*Generator, error) {
	s.Lock()
	defer s.Unlock()

	return s.getOrNewGenerator(serviceName)
}

func (s *Manager) getOrNewGenerator(serviceName string) (*Generator, error) {
	gen, ok := s.generatorMap[serviceName]
	if ok == false {
		var err error
		// not exists, create it.
		gen, err = NewGenerator(s.db, serviceName)
		if err != nil {
			return nil, err
		}

		s.generatorMap[serviceName] = gen
	}

	return gen, nil
}

// ListServices list all exists services
func (s *Manager) ListServices() map[string]int64 {
	mp := make(map[string]int64, len(s.generatorMap))
	for name, gen := range s.generatorMap {
		mp[name] = gen.Current()
	}

	return mp
}

// ServiceExists check service exists
func (s *Manager) ServiceExists(serviceName string) bool {
	s.Lock()
	_, ok := s.generatorMap[serviceName]
	s.Unlock()

	return ok
}

// DelService delete an exists service
func (s *Manager) DelService(serviceName string) error {
	s.Lock()
	gen, ok := s.generatorMap[serviceName]
	if ok {
		delete(s.generatorMap, serviceName)
	}
	s.Unlock()

	// exists
	if ok {
		err := gen.DelKeyTable(serviceName)
		if err != nil {
			return err
		}

		err = s.DelKey(serviceName)
		return err
	}

	return errors.New("service name not exists")
}

// SetServiceId set service latest id
// NOTICE:
// 	if force=FALSE, will not update db value to `lastId`, only update current to db last value.
// 	so, if you want reset db value, must be set force=TRUE
//
// Usage:
//	SetServicesId("service_user", 2300)
//	SetServicesId("service_user", 2300, true)
func (s *Manager) SetServiceId(serviceName string, lastId int64, force bool) (int64,error) {
	gen, err := s.GetOrNewGenerator(serviceName)
	if err != nil {
		return 0, err
	}

	err = s.SetKey(serviceName)
	if err != nil {
		return 0, err
	}

	err = gen.Reset(lastId, force)

	return gen.Current(), err
}

// SetServices set multi service latest ids
// Usage:
//	SetServices({"service_user": 2300, "service_order": 22300})
func (s *Manager) SetMultiServices(kvMap map[string]int64, force bool) ( map[string]int64,  error) {
	ret := make(map[string]int64, len(kvMap))
	for name, lastId := range kvMap {
		lid, err := s.SetServiceId(name, lastId, force)
		if err != nil {
			return ret, err
		}

		ret[name] = lid
	}

	return ret, nil
}

// CurrentId get current id
func (s *Manager) CurrentId(serviceName string) (int64, error) {
	gen, err := s.GetGenerator(serviceName)
	if err != nil {
		return 0, err
	}

	id := gen.Current()
	return id, nil
}

// NextId generate next id
func (s *Manager) NextId(serviceName string) (int64, error) {
	gen, err := s.GetGenerator(serviceName)
	if err != nil {
		return 0, err
	}

	id, err := gen.Next()
	if err != nil {
		return 0, err
	}

	return id, nil
}

// std the default manager
var std = NewEmptyManager()

// Std the default manager
func Std() *Manager {
	return std
}

// InitStdManager init the default manager
func InitStdManager(db *sql.DB) error {
	std.db = db
	return std.Init()
}

// ListServices list all exists services
func ListServices() map[string]int64 { return std.ListServices() }

// GetGenerator list all exists services
func GetGenerator(serviceName string) (*Generator, error) { return std.GetGenerator(serviceName) }

// ServiceExists check service exists
func ServiceExists(serviceName string) bool { return std.ServiceExists(serviceName) }

// NextId generate next id
func NextId(serviceName string) (int64, error) { return std.NextId(serviceName) }

// CurrentId get current id
func CurrentId(serviceName string) (int64, error) { return std.CurrentId(serviceName) }

// SetServiceId set service latest id
func SetServiceId(serviceName string, lastId int64, force bool) (int64,error) {
	return std.SetServiceId(serviceName, lastId, force)
}

// GoodServiceKey check input service name key is valid
func GoodServiceKey(serviceName string) (string, error) {
	serviceName = strings.TrimSpace(serviceName)
	if serviceName == "" {
		return "", errors.New("service key is required")
	}

	return serviceName, nil
}
