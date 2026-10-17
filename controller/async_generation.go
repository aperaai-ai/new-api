package controller

import (
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/model"
	"github.com/QuantumNous/new-api/pkg/asyncprovider"
	"github.com/QuantumNous/new-api/service"
	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

func asyncPublic(j model.AsyncJob) gin.H {
	var output any
	if j.Output != "" {
		_ = common.Unmarshal([]byte(j.Output), &output)
	}
	return gin.H{"id": j.ID, "model": j.Model, "type": j.Kind, "status": j.Status, "progress": j.Progress, "billing_status": j.Billing, "quota": j.Quota, "output": output, "error_code": j.ErrorCode, "created_at": j.CreatedAt, "updated_at": j.UpdatedAt}
}
func CreateAsyncGeneration(c *gin.Context) {
	idem := c.GetHeader("Idempotency-Key")
	if len(strings.TrimSpace(idem)) < 8 || len(idem) > 128 {
		c.JSON(400, gin.H{"error": "Idempotency-Key must contain 8–128 bytes"})
		return
	}
	raw, e := io.ReadAll(io.LimitReader(c.Request.Body, 16385))
	if e != nil || len(raw) > 16384 {
		c.JSON(413, gin.H{"error": "request_too_large"})
		return
	}
	request, e := asyncprovider.Parse(raw)
	if e != nil {
		c.JSON(400, gin.H{"error": e.Error()})
		return
	}
	token := c.MustGet("async_token").(*model.Token)
	if token.ModelLimitsEnabled && !token.GetModelLimitsMap()[request.Model] {
		c.AbortWithStatus(403)
		return
	}
	j, created, e := service.CreateAsyncGeneration(c.Request.Context(), token.UserId, token.Id, c.GetString("async_group"), idem, request)
	if e != nil {
		status, code := 400, "invalid_generation_request"
		if errors.Is(e, service.ErrAsyncRequest) {
			status, code = 400, "invalid_generation_request"
		} else if errors.Is(e, model.ErrAsyncConflict) {
			status, code = 409, "idempotency_conflict"
		} else if errors.Is(e, model.ErrAsyncFunds) {
			status, code = 402, "insufficient_quota"
		} else if errors.Is(e, model.ErrAsyncCapacity) {
			status, code = 429, "queue_full"
		} else {
			common.SysError("async admission failed")
			status, code = 503, "generation_unavailable"
		}
		c.JSON(status, gin.H{"error": code})
		return
	}
	status := http.StatusOK
	if created {
		status = http.StatusAccepted
	}
	c.Header("Location", "/v1/3d/tasks/"+j.ID)
	c.JSON(status, asyncPublic(*j))
}
func GetAsyncGeneration(c *gin.Context) {
	t := c.MustGet("async_token").(*model.Token)
	var j model.AsyncJob
	if e := model.AsyncDB(c.Request.Context()).Where("id = ? AND user_id = ? AND token_id = ?", c.Param("id"), t.UserId, t.Id).First(&j).Error; e != nil {
		if !errors.Is(e, gorm.ErrRecordNotFound) {
			c.AbortWithStatus(503)
			return
		}
		c.AbortWithStatus(404)
		return
	}
	c.JSON(200, asyncPublic(j))
}
func ListAsyncGenerations(c *gin.Context) {
	t := c.MustGet("async_token").(*model.Token)
	var jobs []model.AsyncJob
	q := model.AsyncDB(c.Request.Context()).Where("user_id = ? AND token_id = ?", t.UserId, t.Id)
	if before := c.Query("before"); before != "" {
		var cursor model.AsyncJob
		if model.AsyncDB(c.Request.Context()).Where("id = ? AND user_id = ? AND token_id = ?", before, t.UserId, t.Id).First(&cursor).Error != nil {
			c.AbortWithStatus(400)
			return
		}
		q = q.Where("(created_at < ? OR (created_at = ? AND id < ?))", cursor.CreatedAt, cursor.CreatedAt, cursor.ID)
	}
	if q.Order("created_at DESC, id DESC").Limit(50).Find(&jobs).Error != nil {
		c.AbortWithStatus(503)
		return
	}
	out := make([]gin.H, 0, len(jobs))
	for _, j := range jobs {
		out = append(out, asyncPublic(j))
	}
	c.JSON(200, gin.H{"data": out})
}

// ReconcileAsyncGeneration is root-only, audited, and never creates a new upstream job.
func ReconcileAsyncGeneration(c *gin.Context) {
	var request struct {
		Action       string `json:"action"`
		UpstreamID   string `json:"upstream_task_id"`
		Reason       string `json:"reason"`
		Confirmation string `json:"confirmation"`
	}
	raw, e := io.ReadAll(io.LimitReader(c.Request.Body, 4097))
	if e != nil || len(raw) > 4096 || common.Unmarshal(raw, &request) != nil || len(strings.TrimSpace(request.Reason)) < 16 {
		c.AbortWithStatus(400)
		return
	}
	var j model.AsyncJob
	if model.AsyncDB(c.Request.Context()).Where("id = ? AND status = ? AND upstream_id = ? AND billing = ?", c.Param("id"), model.AsyncReconciling, "", "held").First(&j).Error != nil {
		c.AbortWithStatus(409)
		return
	}
	audit := model.AsyncAudit{JobID: j.ID, ActorID: c.GetInt("id"), Action: request.Action, Reason: request.Reason, CreatedAt: time.Now().Unix()}
	switch request.Action {
	case "attach":
		if request.UpstreamID == "" || len(request.UpstreamID) > 191 {
			c.AbortWithStatus(400)
			return
		}
		key, e := asyncprovider.Open(common.CryptoSecret, j.ID, j.Credential)
		if e != nil {
			c.AbortWithStatus(503)
			return
		}
		provider := service.NewAsyncProvider(j.Provider, key)
		if provider == nil {
			c.AbortWithStatus(503)
			return
		}
		results, e := provider.Poll(c.Request.Context(), []string{request.UpstreamID})
		if e != nil || results[request.UpstreamID].ID != request.UpstreamID {
			c.JSON(400, gin.H{"error": "upstream_task_not_verified"})
			return
		}
		e = model.AttachAsyncUpstream(j, request.UpstreamID, audit)
		if e != nil {
			c.AbortWithStatus(409)
			return
		}
	case "confirm_not_created":
		if request.Confirmation != "I verified that no upstream task was created" {
			c.AbortWithStatus(400)
			return
		}
		if e := model.FinishAsync(&j, false, "operator_confirmed_not_created", "", "", audit); e != nil {
			c.AbortWithStatus(409)
			return
		}
	default:
		c.AbortWithStatus(400)
		return
	}
	c.Status(204)
}

func ListAsyncReconciliation(c *gin.Context) {
	var jobs []model.AsyncJob
	if model.AsyncDB(c.Request.Context()).Where("status = ? AND billing = ?", model.AsyncReconciling, "held").Order("updated_at ASC").Limit(100).Find(&jobs).Error != nil {
		c.AbortWithStatus(503)
		return
	}
	out := make([]gin.H, 0, len(jobs))
	for _, j := range jobs {
		item := asyncPublic(j)
		item["account"] = j.Account
		item["upstream_task_id"] = j.UpstreamID
		item["user_id"] = j.UserID
		out = append(out, item)
	}
	c.JSON(200, gin.H{"data": out})
}
