package main

import (
	"context"
	"mini-ray/common"
	"mini-ray/core/driver"
	"os"
	"syscall"

	"net/http"
	_ "net/http/pprof"

	"github.com/panjf2000/ants/v2"
	"github.com/panjf2000/gnet/v2/pkg/pool/goroutine"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
)

var (
	rootCmd = &cobra.Command{
		Use:        "scheduler",
		Short:      "scheduler for mini-ray",
		SuggestFor: []string{"scheduler"},
		Run:        execute,
	}

	//zap log
	logPath       string
	logMaxAge     int
	logMaxBackups int
	debugEnabled  bool

	pprofEnabled    bool
	pprofListenAddr string

	advertiseAddr string
	listenAddr    string

	metricsEnabled bool

	maxConnections int
)

func execute(cmd *cobra.Command, args []string) {

	lg := initLog(logPath, logMaxAge, logMaxBackups, debugEnabled)
	defer lg.Sync()

	lg.Info("--__V__-- scheduler starting", zap.String("version", "0.0.1"))
	workerPool, err := ants.NewPool(goroutine.DefaultAntsPoolSize, ants.WithOptions(ants.Options{ExpiryDuration: goroutine.ExpiryDuration, Nonblocking: goroutine.Nonblocking, PanicHandler: func(i interface{}) {
		// Do not recover the panic
	}}))
	if err != nil {
		lg.Error("Failed to create worker pool", zap.Error(err))
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if pprofEnabled {
		workerPool.Submit(func() {
			err := http.ListenAndServe(pprofListenAddr, nil)
			lg.Info("Starting pprof", zap.String("addr", pprofListenAddr), zap.Error(err))
		})
	}

	if len(advertiseAddr) == 0 || len(listenAddr) == 0 {
		lg.Error("Failed to get addr", zap.String("advertise", advertiseAddr), zap.String("listen", listenAddr))
		return
	}
	// metrics
	if metricsEnabled {
	}

	// syncer.InstanceInfo.WithLabelValues(advertiseAddr, syncer.Version).Set(1)
	opts := []common.Option{
		common.WithMaxConnections(maxConnections),
	}

	server := driver.NewDriver(ctx, lg, workerPool, listenAddr, advertiseAddr, opts...)
	server.Start()
	workerPool.Submit(server.Loop)
	common.RegisterSignal(ctx, syscall.SIGINT, syscall.SIGTERM)
	lg.Info("Goodbye")
}

func initLog(path string, maxAge int, maxBackups int, debugEnable bool) *zap.Logger {
	var lcfg zap.Config
	if debugEnable {
		lcfg = zap.NewDevelopmentConfig()
	} else {
		lcfg = zap.NewProductionConfig()
		lcfg.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	}
	cores := []zapcore.Core{zapcore.NewCore(zapcore.NewJSONEncoder(lcfg.EncoderConfig), zapcore.Lock(os.Stdout), zapcore.InfoLevel)}
	if len(path) > 0 {
		l := &lumberjack.Logger{
			Filename:  path,
			MaxSize:   50,
			MaxAge:    maxAge,
			LocalTime: true,
			Compress:  true,
		}
		if maxBackups != 0 {
			l.MaxBackups = maxBackups
		}
		cores = append(cores, zapcore.NewCore(zapcore.NewJSONEncoder(lcfg.EncoderConfig), zapcore.AddSync(zapcore.AddSync(l)), lcfg.Level))
	}
	lg := zap.New(zapcore.NewTee(cores...), zap.AddCaller())
	return lg
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func init() {

}
