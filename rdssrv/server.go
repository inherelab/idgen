package rdssrv

import (
	"net"
	"runtime"

	"github.com/gookit/slog"
	"github.com/inherelab/genid/mysqlid"
)

// Server struct
type Server struct {
	*mysqlid.Manager

	addr string

	// mark server is running
	running  bool
	listener net.Listener
}

// NewServer create an new server
func NewServer(addr string, mgr *mysqlid.Manager) (*Server, error) {
	var err error
	s := &Server{
		addr: addr,
	}

	s.Manager = mgr

	netProto := "tcp"
	s.listener, err = net.Listen(netProto, addr)
	if err != nil {
		return nil, err
	}

	slog.Info("Server created with protocol:", netProto, "and Listen On:", s.addr)
	return s, err
}

// Serve running
func (s *Server) Serve() error {
	if err := s.Init(); err != nil {
		return err
	}

	s.running = true
	for s.running {
		conn, err := s.listener.Accept()
		if err != nil {
			slog.Error("server accept error", err.Error())
			continue
		}

		go s.onConn(conn)
	}
	return nil
}

func (s *Server) onConn(conn net.Conn) {
	defer func() {
		clientAddr := conn.RemoteAddr().String()
		r := recover()
		if err, ok := r.(error); ok {
			const size = 4096
			buf := make([]byte, size)
			buf = buf[:runtime.Stack(buf, false)] // 获得当前goroutine的stacktrace
			slog.Error("server onConn error",
				"remoteAddr", clientAddr,
				"stack", string(buf),
				"err", err.Error(),
			)

			reply := &ErrorReply{
				message: err.Error(),
			}

			// TODO ignore error
			_, _ = reply.WriteTo(conn)
		}

		// TODO ignore error
		_= conn.Close()
	}()

	for {
		request, err := NewRequest(conn)
		if err != nil {
			slog.Error("new request error", err)
			return
		}

		reply := s.ServeRequest(request)
		if _, err := reply.WriteTo(conn); err != nil {
			slog.Error("reply write error", err)
			return
		}
	}
}

// ServeRequest handle request
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
	if s.listener != nil {
		err := s.listener.Close()
		if err != nil {
			slog.Error(err)
		}
	}

	slog.Info("redis server closed!")
}
