package common

import (
	"net/url"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAperaConnectionSecretsAreEscapedAndTLSVerified(t *testing.T) {
	t.Setenv("SQL_HOST", "database.example.com")
	t.Setenv("SQL_USER", "apera_app")
	t.Setenv("SQL_DATABASE", "apera")
	t.Setenv("SQL_PASSWORD", "p@ss/?&word")
	t.Setenv("SQL_SSL_ROOT_CERT", "/certs/rds.pem")
	t.Setenv("SQL_DSN", "")
	t.Setenv("REDIS_CONN_STRING", "rediss://redis.example.com:6379/0")
	t.Setenv("REDIS_AUTH_TOKEN", "redis:@secret")
	require.NoError(t, ConfigureAperaConnections())
	pg, e := url.Parse(os.Getenv("SQL_DSN"))
	require.NoError(t, e)
	password, _ := pg.User.Password()
	require.Equal(t, "p@ss/?&word", password)
	require.Equal(t, "verify-full", pg.Query().Get("sslmode"))
	redis, e := url.Parse(os.Getenv("REDIS_CONN_STRING"))
	require.NoError(t, e)
	password, _ = redis.User.Password()
	require.Equal(t, "redis:@secret", password)
}
