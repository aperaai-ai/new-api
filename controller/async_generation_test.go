package controller

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/QuantumNous/new-api/model"
	"github.com/gin-gonic/gin"
	"github.com/glebarez/sqlite"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func TestAsyncTaskIsolationAndSecretRedaction(t *testing.T) {
	db, e := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	require.NoError(t, e)
	saved := model.DB
	model.DB = db
	t.Cleanup(func() { sqlDB, _ := db.DB(); _ = sqlDB.Close(); model.DB = saved })
	require.NoError(t, model.MigrateAsyncJobs())
	require.NoError(t, db.Create(&model.AsyncJob{ID: "job-secret", IdempotencyHash: "hash", UserID: 1, TokenID: 2, Credential: "secret-provider-key", RequestBody: "private-prompt", UpstreamID: "task_internal", Status: model.AsyncRunning, Billing: "held"}).Error)
	for _, tc := range []struct{ user, token, status int }{{1, 2, 200}, {9, 2, 404}, {1, 9, 404}} {
		router := gin.New()
		router.GET("/tasks/:id", func(c *gin.Context) { c.Set("async_token", &model.Token{Id: tc.token, UserId: tc.user}) }, GetAsyncGeneration)
		recorder := httptest.NewRecorder()
		router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/tasks/job-secret", nil))
		require.Equal(t, tc.status, recorder.Code)
		for _, secret := range []string{"secret-provider-key", "private-prompt", "task_internal"} {
			require.NotContains(t, recorder.Body.String(), secret)
		}
	}
}
