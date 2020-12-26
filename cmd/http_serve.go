package cmd

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/gookit/gcli/v2"
	"github.com/gookit/slog"
	"github.com/inherelab/genid/httpsrv"
	"github.com/inherelab/genid/mysqlid"
)

var httpSrvOpts = struct {
	addr     string
	config   string
	logLevel string
}{}

var HttpServeCommand = &gcli.Command{
	Name:    "http",
	Aliases: []string{"http-serve", "http-server"},
	UseFor:  "start an ID generator http server",
	Config: func(c *gcli.Command) {
		c.StrOpt(&httpSrvOpts.addr, "addr", "a", "127.0.0.1:9090", "the server listen address")
		c.StrOpt(&httpSrvOpts.config, "config", "c", "config/config.toml", "the server config file")
		c.StrOpt(&httpSrvOpts.logLevel, "log-level", "l", "error", "log level. allow: debug|info|warn|error")
	},
	Func: func(c *gcli.Command, args []string) error {
		err := prepare(httpSrvOpts.config)
		if err != nil {
			return err
		}

		setLogLevel(httpSrvOpts.logLevel)

		// init mysqlId generator manager
		slog.Info("init the default mysqlId generator manager")
		err = mysqlid.InitStdManager(mysqlid.Db)
		if err != nil {
			slog.Fatal(err)
		}

		s := httpsrv.NewServer(mysqlid.Std(), httpSrvOpts.addr)

		sc := make(chan os.Signal, 1)
		signal.Notify(sc, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

		go func() {
			sig := <-sc
			slog.Info("Got system signal:", sig)
			s.Close()
		}()

		slog.Info("ID generator http server started")

		return s.Serve()
	},
}
