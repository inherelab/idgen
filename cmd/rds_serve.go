package cmd

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/gookit/gcli/v2"
	"github.com/gookit/slog"
	"github.com/inherelab/genid/mysqlid"
	"github.com/inherelab/genid/rdssrv"
)

var rdsSrvOpts = struct {
	addr     string
	config   string
	logLevel string
}{}

var RdsServeCommand = &gcli.Command{
	Name:    "redis",
	Aliases: []string{"rds", "rdssrv", "rds-server"},
	UseFor:  "start an ID generator server like redis",
	Config: func(c *gcli.Command) {
		c.StrOpt(&rdsSrvOpts.addr, "addr", "a", "127.0.0.1:6379", "the server listen address")
		c.StrOpt(&rdsSrvOpts.config, "config", "c", "config/config.toml", "the server config file")
		c.StrOpt(&rdsSrvOpts.logLevel, "log-level", "l", "error", "log level. allow: debug|info|warn|error")
	},
	Func: func(c *gcli.Command, args []string) error {
		err := prepare(httpSrvOpts.config)
		if err != nil {
			return err
		}

		setLogLevel(rdsSrvOpts.logLevel)

		// init mysqlId generator manager
		slog.Info("init the default mysqlId generator manager")
		err = mysqlid.InitStdManager(mysqlid.Db)
		if err != nil {
			return err
		}

		slog.Info("create the custom redis server")
		// var s *rdssrv.Server
		s, err := rdssrv.NewServer(rdsSrvOpts.addr, mysqlid.Std())
		if err != nil {
			slog.Error(err)
			// slog.Flush()
			// s.Close()
			return err
		}

		sc := make(chan os.Signal, 1)
		signal.Notify(sc, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

		go func() {
			sig := <-sc
			slog.Info("Got signal:", sig)
			s.Close()
		}()
		slog.Info("ID generator redis server started")

		return s.Serve()
	},
}

func setLogLevel(level string) {
	logLevel, err := slog.Name2Level(level)
	if err != nil {
		logLevel = slog.ErrorLevel
	}

	slog.Configure(func(logger *slog.SugaredLogger) {
		logger.Level = logLevel
	})
}
