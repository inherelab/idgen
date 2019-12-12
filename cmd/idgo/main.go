package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path"
	"runtime"
	"strings"
	"syscall"

	"github.com/flike/golog"
	"github.com/flike/idgo/server"
)

var configFile = flag.String("config", "config/idgo.toml", "idgo config file")
var logLevel = flag.String("log-level", "error", "log level [debug|info|warn|error], default error")

const (
	sysLogName = "sys.log"
	MaxLogSize = 1024 * 1024 * 1024
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	flag.Parse()

	if len(*configFile) == 0 {
		fmt.Println("must use a config file")
		return
	}

	cfg, err := server.ParseConfigFile(*configFile)
	if err != nil {
		fmt.Printf("parse config file error:%v\n", err.Error())
		return
	}

	// when the log file size greater than 1GB, kingtask will generate a new file
	if len(cfg.LogPath) != 0 {
		sysFilePath := path.Join(cfg.LogPath, sysLogName)
		sysFile, err := golog.NewRotatingFileHandler(sysFilePath, MaxLogSize, 1)
		if err != nil {
			fmt.Printf("new log file error:%v\n", err.Error())
			return
		}
		golog.GlobalLogger = golog.New(sysFile, golog.Lfile|golog.Ltime|golog.Llevel)
	}

	if *logLevel != "" {
		setLogLevel(*logLevel)
	} else {
		setLogLevel(cfg.LogLevel)
	}

	err = startAndRunServer(cfg)
	if err != nil {
		golog.Error("main", "main", err.Error(), 0)
		golog.GlobalLogger.Close()
		fmt.Println(err.Error())
	}
}

func startAndRunServer(cfg *server.Config) (err error) {
	var s *server.Server

	// create server
	s, err = server.NewServer(cfg)
	if err != nil {
		s.Close()
		return
	}

	// init server
	if err = s.Init(); err != nil {
		s.Close()
		return
	}

	// listen signals
	sc := make(chan os.Signal, 1)
	signal.Notify(sc, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	go func() {
		sig := <-sc
		golog.Info("main", "main", "Got signal", 0, "signal", sig)
		golog.GlobalLogger.Close()
		s.Close()
	}()

	// server run
	golog.Info("main", "main", "Idgo server started", 0)
	return s.Serve()
}

func setLogLevel(level string) {
	switch strings.ToLower(level) {
	case "debug":
		golog.GlobalLogger.SetLevel(golog.LevelDebug)
	case "info":
		golog.GlobalLogger.SetLevel(golog.LevelInfo)
	case "warn":
		golog.GlobalLogger.SetLevel(golog.LevelWarn)
	case "error":
		golog.GlobalLogger.SetLevel(golog.LevelError)
	default:
		golog.GlobalLogger.SetLevel(golog.LevelError)
	}
}
