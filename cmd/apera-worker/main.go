package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/logger"
	"github.com/QuantumNous/new-api/model"
	"github.com/QuantumNous/new-api/pkg/modelqueue"
	"github.com/QuantumNous/new-api/service"
)

func main() {
	_ = os.Setenv("NODE_TYPE", "slave")
	common.InitEnv()
	logger.SetupLogger()
	if e := common.ConfigureAperaConnections(); e != nil {
		log.Fatal(e)
	}
	if os.Getenv("SQL_DSN") == "" {
		log.Fatal("worker requires SQL_DSN or SQL_HOST")
	}
	if e := model.InitDB(); e != nil {
		log.Fatal("worker database initialization failed")
	}
	defer model.CloseDB()
	if e := model.InitLogDB(); e != nil {
		log.Fatal("worker log database initialization failed")
	}
	if e := common.InitRedisClient(); e != nil {
		log.Fatal("worker Redis initialization failed")
	}
	service.InitHttpClient()
	if e := modelqueue.Init(context.Background()); e != nil {
		log.Fatal(e)
	}
	if e := service.LoadAsyncConfig(); e != nil {
		log.Fatal(e)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	if modelqueue.Enabled() {
		go modelqueue.Default.RunPublisher(ctx)
		go modelqueue.Default.RunWorker(ctx)
	}
	var progress atomic.Int64
	progress.Store(time.Now().Unix())
	mux := http.NewServeMux()
	mux.HandleFunc("/health/ready", func(w http.ResponseWriter, r *http.Request) {
		c, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		db, e := model.DB.DB()
		if e != nil || db.PingContext(c) != nil || common.RedisEnabled && common.RDB.Ping(c).Err() != nil || time.Now().Unix()-progress.Load() > 90 {
			w.WriteHeader(503)
			return
		}
		w.WriteHeader(200)
	})
	server := &http.Server{Addr: ":3000", Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if e := server.ListenAndServe(); e != nil && e != http.ErrServerClosed {
			log.Print("worker health server failed")
			stop()
		}
	}()
	lastMetrics := time.Time{}
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_ = server.Shutdown(shutdown)
			cancel()
			return
		case <-ticker.C:
			if e := service.AsyncWorkerStep(ctx); e != nil {
				common.SysError("async worker step failed")
			}
			progress.Store(time.Now().Unix())
			if time.Since(lastMetrics) > 30*time.Second {
				if e := service.LogAsyncMetrics(ctx); e != nil {
					common.SysError("async metrics query failed")
				}
				lastMetrics = time.Now()
			}
		}
	}
}
