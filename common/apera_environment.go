package common

import (
	"fmt"
	"net/url"
	"os"
)

// ConfigureAperaConnections assembles connection URLs from ECS-injected secrets.
// URL escaping prevents passwords from changing connection parameters.
func ConfigureAperaConnections() error {
	if host := os.Getenv("SQL_HOST"); host != "" {
		password := os.Getenv("SQL_PASSWORD")
		user := os.Getenv("SQL_USER")
		db := os.Getenv("SQL_DATABASE")
		ca := os.Getenv("SQL_SSL_ROOT_CERT")
		if password == "" || user == "" || db == "" || ca == "" {
			return fmt.Errorf("SQL_HOST requires SQL_USER, SQL_PASSWORD, SQL_DATABASE and SQL_SSL_ROOT_CERT")
		}
		port := os.Getenv("SQL_PORT")
		if port == "" {
			port = "5432"
		}
		u := url.URL{Scheme: "postgres", Host: host + ":" + port, User: url.UserPassword(user, password), Path: "/" + db}
		q := u.Query()
		q.Set("sslmode", "verify-full")
		q.Set("connect_timeout", "5")
		q.Set("options", "-c statement_timeout=15000 -c lock_timeout=5000")
		q.Set("sslrootcert", ca)
		u.RawQuery = q.Encode()
		if e := os.Setenv("SQL_DSN", u.String()); e != nil {
			return e
		}
	}
	if token := os.Getenv("REDIS_AUTH_TOKEN"); token != "" {
		u, e := url.Parse(os.Getenv("REDIS_CONN_STRING"))
		if e != nil || u.Scheme != "rediss" || u.Host == "" {
			return fmt.Errorf("Redis auth requires a rediss URL")
		}
		u.User = url.UserPassword("", token)
		return os.Setenv("REDIS_CONN_STRING", u.String())
	}
	return nil
}
