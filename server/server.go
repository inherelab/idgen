package server

import (
	"database/sql"
	"fmt"
	"net"
	"runtime"
	"sync"

	"github.com/flike/golog"
)

const (
	// move to config
	// KeyRecordTableName         = "__idgo__"

	CreateRecordTableSQLFormat = `
	CREATE TABLE %s (
    k VARCHAR(128) NOT NULL,
    PRIMARY KEY (k)
) ENGINE=Innodb DEFAULT CHARSET=utf8 `

	// create key table if not exist
	CreateRecordTableNTSQLFormat = `
	CREATE TABLE IF NOT EXISTS %s (
    k VARCHAR(128) NOT NULL,
    PRIMARY KEY (k)
) ENGINE=Innodb DEFAULT CHARSET=utf8 `

	InsertKeySQLFormat  = "INSERT INTO %s (k) VALUES ('%s')"
	SelectKeySQLFormat  = "SELECT k FROM %s WHERE k = '%s'"
	SelectKeysSQLFormat = "SELECT k FROM %s"
	DeleteKeySQLFormat  = "DELETE FROM %s WHERE k = '%s'"
)

// Server struct definition
type Server struct {
	db  *sql.DB

	tcpListener  net.Listener
	generatorMap map[string]*MySQLIdGenerator

	running bool
	sync.RWMutex
}

// NewServer create new server
func NewServer() (*Server, error) {
	var err error

	s := new(Server)

	// open db
	err = s.openDbConn()

	return s, err
}

// open db
func (s *Server) openDbConn() (err error) {
	// init db
	proto := "mysql"
	charset := "utf8"

	// root:@tcp(127.0.0.1:3306)/test?charset=utf8
	url := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?%s",
		cfg.DbConfig.User,
		cfg.DbConfig.Password,
		cfg.DbConfig.Host,
		cfg.DbConfig.Port,
		cfg.DbConfig.DBName,
		charset,
	)

	logInfo("NewServer", "begin open mysql connection")
	s.db, err = sql.Open(proto, url)
	if err != nil {
		logError("openDbConn", "open database error","err", err.Error())
	}

	return
}

// Init server
func (s *Server) Init() error {
	createTableNtSQL := fmt.Sprintf(CreateRecordTableNTSQLFormat, cfg.TableName)
	selectKeysSQL := fmt.Sprintf(SelectKeysSQLFormat, cfg.TableName)
	_, err := s.db.Exec(createTableNtSQL)
	if err != nil {
		return err
	}

	s.generatorMap = make(map[string]*MySQLIdGenerator)
	rows, err := s.db.Query(selectKeysSQL)
	if err != nil {
		return err
	}

	defer rows.Close()
	for rows.Next() {
		idGenKey := ""
		err := rows.Scan(&idGenKey)
		if err != nil {
			return err
		}

		if idGenKey != "" {
			idgen, ok := s.generatorMap[idGenKey]
			if ok == false {
				isExist, err := s.IsKeyExist(idGenKey)
				if err != nil {
					return err
				}
				if isExist {
					idgen, err = NewMySQLIdGenerator(s.db, idGenKey)
					if err != nil {
						return err
					}
					s.generatorMap[idGenKey] = idgen
				}
			}
		}
	}
	return nil
}

// Serve receive connection
func (s *Server) Serve() (err error) {
	s.running = true

	// create an tcp listen serve
	netProto := "tcp"
	logInfo("Serve", "begin create an tcp listen serve")
	s.tcpListener, err = net.Listen(netProto, cfg.Addr)
	if err != nil {
		return err
	}

	logInfo("Serve", "Server running","netProto",
		netProto,
		"address",
		cfg.Addr,
	)

	logInfo("Serve", "Idgo server started", 0)
	// for loop to receive conn
	for s.running {
		conn, err := s.tcpListener.Accept()
		if err != nil {
			golog.Error("server", "Run", err.Error(), 0)
			continue
		}

		// handle conn
		go s.onConn(conn)
	}
	return nil
}

// handle connection
func (s *Server) onConn(conn net.Conn) error {
	defer func() {
		clientAddr := conn.RemoteAddr().String()
		r := recover()
		if err, ok := r.(error); ok {
			const size = 4096
			buf := make([]byte, size)
			buf = buf[:runtime.Stack(buf, false)] // 获得当前goroutine的stacktrace
			golog.Error("server", "onConn", "error", 0,
				"remoteAddr", clientAddr,
				"stack", string(buf),
				"err", err.Error(),
			)
			reply := &ErrorReply{
				message: err.Error(),
			}
			reply.WriteTo(conn)
		}
		conn.Close()
	}()

	// for loop
	for {
		request, err := NewRequest(conn)
		if err != nil {
			return err
		}

		reply := s.ServeRequest(request)
		if _, err := reply.WriteTo(conn); err != nil {
			golog.Error("server", "onConn", "reply write error", 0,
				"err", err.Error())
			return err
		}
	}
}

func (s *Server) ServeRequest(request *Request) Reply {
	switch request.Command {
	case "GET":
		return s.handleGet(request)
	case "SET":
		return s.handleSet(request)
	case "EXISTS":
		return s.handleExists(request)
	case "DEL":
		return s.handleDel(request)
	case "SELECT":
		return s.handleSelect(request)
	default:
		return ErrMethodNotSupported
	}
}

// Close server
func (s *Server) Close() {
	s.running = false
	if s.tcpListener != nil {
		s.tcpListener.Close()
	}

	golog.Info("server", "close", "server closed!", 0)
}

func (s *Server) IsKeyExist(key string) (bool, error) {
	var tableName string
	var haveValue bool
	if len(key) == 0 {
		return false, nil
	}
	getKeySQL := fmt.Sprintf(GetKeySQLFormat, getTableName(key))
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

// Get value by key
func (s *Server) GetKey(key string) (string, error) {
	keyName := ""

	selectKeySQL := fmt.Sprintf(SelectKeySQLFormat, cfg.TableName, key)
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
		return keyName, fmt.Errorf("%s: not exists key", key)
	}
	return keyName, nil
}

// SetKey set key
func (s *Server) SetKey(key string) error {
	if len(key) == 0 {
		return fmt.Errorf("%s:invalid key", key)
	}
	_, err := s.GetKey(key)
	if err == nil {
		return nil
	} else {
		insertKeySQL := fmt.Sprintf(InsertKeySQLFormat, cfg.TableName, key)
		_, err = s.db.Exec(insertKeySQL)
		if err != nil {
			return err
		}
		return nil
	}
}

// DelKey delete key
func (s *Server) DelKey(key string) error {
	if len(key) == 0 {
		return fmt.Errorf("key is cannot be empty")
	}

	_, err := s.GetKey(key)
	if err == nil {
		deleteKeySQL := fmt.Sprintf(DeleteKeySQLFormat, cfg.TableName, key)

		_, err = s.db.Exec(deleteKeySQL)
		if err != nil {
			return err
		}
		return nil
	}

	return err
}

func logInfo(method, msg string, args ...interface{})  {
	golog.Info("Server", method, msg, 0, args...)
}

func logError(method, msg string, args ...interface{})  {
	golog.Error("Server", method, msg, 0, args...)
}
