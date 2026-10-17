package middleware

import (
	"errors"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/model"
	"github.com/QuantumNous/new-api/service"
	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

// AsyncTokenAuth permits depleted (but never disabled/expired) keys to retrieve
// existing paid jobs and replay idempotent requests. New jobs recheck funds in SQL.
func AsyncTokenAuth() gin.HandlerFunc {
	return func(c *gin.Context) {
		if !service.AsyncSettings.Enabled {
			c.AbortWithStatusJSON(503, gin.H{"error": "async_generation_disabled"})
			return
		}
		fields := strings.Fields(c.GetHeader("Authorization"))
		if len(fields) != 2 || !strings.EqualFold(fields[0], "Bearer") {
			c.AbortWithStatus(http.StatusUnauthorized)
			return
		}
		key := strings.TrimPrefix(fields[1], "sk-")
		var token model.Token
		if key == "" {
			c.AbortWithStatus(401)
			return
		}
		if e := model.AsyncDB(c.Request.Context()).Where(model.Token{Key: key}).First(&token).Error; e != nil {
			if !errors.Is(e, gorm.ErrRecordNotFound) {
				c.AbortWithStatus(503)
				return
			}
			c.AbortWithStatus(401)
			return
		}
		if (token.Status != common.TokenStatusEnabled && token.Status != common.TokenStatusExhausted) || (token.ExpiredTime != -1 && token.ExpiredTime < time.Now().Unix()) {
			c.AbortWithStatus(401)
			return
		}
		ips := token.GetIpLimits()
		if len(ips) > 0 && !common.IsIpInCIDRList(net.ParseIP(c.ClientIP()), ips) {
			c.AbortWithStatus(403)
			return
		}
		var user model.User
		if model.AsyncDB(c.Request.Context()).First(&user, token.UserId).Error != nil || user.Status != common.UserStatusEnabled {
			c.AbortWithStatus(403)
			return
		}
		group := user.Group
		if token.Group != "" {
			if _, ok := service.GetUserUsableGroups(user.Group)[token.Group]; !ok {
				c.AbortWithStatus(403)
				return
			}
			group = token.Group
		}
		// No auto-group fallback for durable jobs: the price/group is pinned at creation.
		if group == "auto" {
			c.AbortWithStatusJSON(400, gin.H{"error": "explicit_token_group_required"})
			return
		}
		ok, e := service.AsyncRate(c.Request.Context(), "user:"+service.AsyncHash(key), 600)
		if e != nil {
			c.AbortWithStatus(503)
			return
		}
		if !ok {
			c.AbortWithStatus(429)
			return
		}
		c.Set("async_token", &token)
		c.Set("async_user", &user)
		c.Set("async_group", group)
		c.Next()
	}
}
