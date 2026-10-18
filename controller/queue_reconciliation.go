package controller

import (
	"net/http"
	"strings"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/model"
	"github.com/gin-gonic/gin"
)

// These routes require RootAuth. Bodies, supplier credentials and object contents
// are never returned by the reconciliation list.
func ListQueueHolds(c *gin.Context) {
	var holds []model.QueueHold
	if e := model.AsyncDB(c.Request.Context()).Where("state = ? AND created_at < ?", "held", time.Now().Unix()-900).Order("created_at ASC").Limit(100).Find(&holds).Error; e != nil {
		c.JSON(503, gin.H{"error": "hold lookup unavailable"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": holds})
}
func ReconcileQueueHold(c *gin.Context) {
	var input struct {
		Action       string `json:"action"`
		Quota        *int   `json:"quota"`
		Reason       string `json:"reason"`
		Confirmation string `json:"confirmation"`
	}
	if common.DecodeJson(c.Request.Body, &input) != nil || input.Confirmation != "provider_verified" || len(strings.TrimSpace(input.Reason)) < 16 || input.Quota == nil || *input.Quota < 0 || (input.Action != "settle" && input.Action != "refund") || (input.Action == "refund" && *input.Quota != 0) {
		c.JSON(400, gin.H{"error": "provider_verified confirmation, reason and non-negative quota are required; refund quota must be zero"})
		return
	}
	state := "settled"
	if input.Action == "refund" {
		state = "released"
	}
	audit := model.QueueHoldAudit{ActorID: c.GetInt("id"), Reason: strings.TrimSpace(input.Reason)}
	if e := model.AdjustQueueHold(c.Request.Context(), c.Param("id"), *input.Quota, state, audit); e != nil {
		c.JSON(409, gin.H{"error": "hold cannot be reconciled: inspect active execution, balance or previous settlement"})
		return
	}
	c.JSON(200, gin.H{"success": true})
}
