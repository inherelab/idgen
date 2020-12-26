package httpsrv

import (
	"net/http"

	"github.com/gookit/slog"
	"github.com/inherelab/genid/mysqlid"
)

// Server struct
type Server struct {
	*mysqlid.Manager

	// addr string
	running bool
	hserver *http.Server
}

// NewServer instance
func NewServer(manager *mysqlid.Manager, addr string) *Server {
	return &Server{
		Manager: manager,
		// addr: addr,
		hserver: &http.Server{
			Addr: addr,
		},
	}
}

// SetHandler for http server
func (s *Server) SetHandler(h http.Handler) {
	s.hserver.Handler = h
}

// Serve running
func (s *Server) Serve() error {
	if err := s.Init(); err != nil {
		return err
	}

	s.running = true
	err := s.hserver.ListenAndServe()
	if err != nil && err != http.ErrServerClosed {
		// Shutdown
		slog.Error("Server closed unexpected", err)
		return err
	}

	return nil
}
