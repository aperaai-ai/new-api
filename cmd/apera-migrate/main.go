package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/model"
	"gorm.io/gorm/clause"
)

func main() {
	_ = os.Setenv("NODE_TYPE", "slave")
	common.InitEnv()
	if e := common.ConfigureAperaConnections(); e != nil {
		log.Fatal(e)
	}
	if os.Getenv("SQL_DSN") == "" {
		log.Fatal("migration requires explicit SQL connection")
	}
	if e := model.InitDB(); e != nil {
		log.Fatal("database connection failed")
	}
	db, e := model.DB.DB()
	if e != nil {
		log.Fatal("database unavailable")
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	conn, e := db.Conn(ctx)
	if e != nil {
		log.Fatal("migration connection unavailable")
	}
	defer conn.Close()
	if common.UsingPostgreSQL {
		if _, e = conn.ExecContext(ctx, "SELECT pg_advisory_lock(615432108)"); e != nil {
			log.Fatal("migration lock failed")
		}
		defer conn.ExecContext(context.Background(), "SELECT pg_advisory_unlock(615432108)")
	}
	if e = model.RunAperaMigrations(); e != nil {
		log.Fatal("schema migration failed; inspect migration logs")
	}
	if password := os.Getenv("APP_DB_PASSWORD"); password != "" {
		if !common.UsingPostgreSQL {
			log.Fatal("application-role provisioning requires PostgreSQL")
		}
		var exists bool
		if e = conn.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname='apera_app')").Scan(&exists); e != nil {
			log.Fatalf("role lookup failed: %v", e)
		}
		verb := "CREATE"
		if exists {
			verb = "ALTER"
		}
		// database/sql avoids ORM SQL logging accidentally exposing this password.
		statement := fmt.Sprintf("%s ROLE apera_app LOGIN PASSWORD '%s'", verb, strings.ReplaceAll(password, "'", "''"))
		if _, e = conn.ExecContext(ctx, statement); e != nil {
			log.Fatal("application role provisioning failed")
		}
		for _, sql := range []string{
			"GRANT CONNECT ON DATABASE apera TO apera_app",
			"GRANT USAGE ON SCHEMA public TO apera_app",
			"GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA public TO apera_app",
			"GRANT USAGE,SELECT ON ALL SEQUENCES IN SCHEMA public TO apera_app",
			"ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT,INSERT,UPDATE,DELETE ON TABLES TO apera_app",
			"ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT USAGE,SELECT ON SEQUENCES TO apera_app",
		} {
			if _, e = conn.ExecContext(ctx, sql); e != nil {
				log.Fatal("application role grant failed")
			}
		}
	}

	if password := os.Getenv("WORKER_DB_PASSWORD"); password != "" {
		if !common.UsingPostgreSQL {
			log.Fatal("worker-role provisioning requires PostgreSQL")
		}
		var exists bool
		if e = conn.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname='apera_worker')").Scan(&exists); e != nil {
			log.Fatalf("worker role lookup failed: %v", e)
		}
		verb := "CREATE"
		if exists {
			verb = "ALTER"
		}
		if _, e = conn.ExecContext(ctx, fmt.Sprintf("%s ROLE apera_worker LOGIN PASSWORD '%s'", verb, strings.ReplaceAll(password, "'", "''"))); e != nil {
			log.Fatal("worker role provisioning failed")
		}
		for _, sql := range []string{
			"REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public FROM apera_worker",
			"GRANT CONNECT ON DATABASE apera TO apera_worker",
			"GRANT USAGE ON SCHEMA public TO apera_worker",
			"GRANT SELECT,INSERT,UPDATE,DELETE ON queue_executions,queue_outboxes,async_accounts TO apera_worker",
			"GRANT SELECT ON async_jobs TO apera_worker",
			"GRANT UPDATE(status,upstream_id,error_code,progress,output,credits,next_attempt,submitted_at,updated_at,version,attempts,execution_done) ON async_jobs TO apera_worker",
			"GRANT INSERT ON async_result_events TO apera_worker",
		} {
			if _, e = conn.ExecContext(ctx, sql); e != nil {
				log.Fatal("worker role grant failed")
			}
		}
	}
	if !model.RootUserExists() {
		password := os.Getenv("APERA_INITIAL_ADMIN_PASSWORD")
		if len(password) < 16 {
			log.Fatal("initial admin password must be supplied before public startup")
		}
		hash, e := common.Password2Hash(password)
		if e != nil {
			log.Fatal("admin initialization failed")
		}
		if e = model.DB.Create(&model.User{Username: "root", DisplayName: "Apera Admin", Password: hash, Role: common.RoleRootUser, Status: common.UserStatusEnabled, Quota: 0}).Error; e != nil {
			log.Fatal("admin creation failed")
		}
	}
	if publicURL := os.Getenv("APERA_PUBLIC_URL"); publicURL != "" {
		if e = model.DB.Clauses(clause.OnConflict{DoNothing: true}).Create(&model.Option{Key: "ServerAddress", Value: publicURL}).Error; e != nil {
			log.Fatal("public address initialization failed")
		}
	}
	model.CheckSetup()
	log.Print("migration completed")
}
