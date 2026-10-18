package router

import (
	"context"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/controller"
	"github.com/QuantumNous/new-api/middleware"
	"github.com/QuantumNous/new-api/model"
	"github.com/gin-gonic/gin"
)

func SetAsyncRouter(r *gin.Engine) {
	r.GET("/health/ready", func(c *gin.Context) {
		ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
		defer cancel()
		db, e := model.DB.DB()
		if e != nil || db.PingContext(ctx) != nil {
			c.Status(503)
			return
		}
		if common.RedisEnabled && common.RDB.Ping(ctx).Err() != nil {
			c.Status(503)
			return
		}
		c.Status(200)
	})
	r.GET("/api/model-queue/holds", middleware.RootAuth(), controller.ListQueueHolds)
	r.POST("/api/model-queue/holds/:id/reconcile", middleware.RootAuth(), controller.ReconcileQueueHold)
	r.GET("/api/async/tasks/reconciling", middleware.RootAuth(), controller.ListAsyncReconciliation)
	r.POST("/api/async/tasks/:id/reconcile", middleware.RootAuth(), controller.ReconcileAsyncGeneration)
	g := r.Group("/v1/3d", middleware.AsyncTokenAuth())
	g.POST("/generations", controller.CreateAsyncGeneration)
	g.GET("/tasks/:id", controller.GetAsyncGeneration)
	g.GET("/tasks", controller.ListAsyncGenerations)
}
