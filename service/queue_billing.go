package service

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/model"
	"github.com/QuantumNous/new-api/pkg/modelqueue"
	relaycommon "github.com/QuantumNous/new-api/relay/common"
	"github.com/QuantumNous/new-api/types"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

type queueBilling struct {
	id       string
	reserved int
	trace    *modelqueue.Trace
}

func newQueueBilling(c *gin.Context, quota int, info *relaycommon.RelayInfo) *types.NewAPIError {
	if info.IsPlayground {
		return types.NewErrorWithStatusCode(errors.New("queued billing requires an API token"), types.ErrorCodeInvalidRequest, http.StatusBadRequest, types.ErrOptionWithSkipRetry())
	}
	h := &model.QueueHold{ID: "hold_" + uuid.NewString(), UserID: info.UserId, TokenID: info.TokenId, Reserved: quota, RequestID: info.RequestId}
	if e := model.CreateQueueHold(c.Request.Context(), h); e != nil {
		return types.NewErrorWithStatusCode(e, types.ErrorCodeInsufficientUserQuota, http.StatusForbidden, types.ErrOptionWithSkipRetry())
	}
	trace := modelqueue.TraceFrom(c.Request.Context())
	if trace == nil {
		ctx, t := modelqueue.WithTrace(c.Request.Context())
		trace = t
		c.Request = c.Request.WithContext(ctx)
	}
	trace.SetHold(h.ID)
	info.Billing = &queueBilling{id: h.ID, reserved: quota, trace: trace}
	info.FinalPreConsumedQuota = quota
	info.BillingSource = BillingSourceWallet
	return nil
}
func (q *queueBilling) GetPreConsumedQuota() int { return q.reserved }
func (q *queueBilling) NeedsRefund() bool        { return true }
func (q *queueBilling) Reserve(target int) error {
	if target <= q.reserved {
		return nil
	}
	if e := model.AdjustQueueHold(context.Background(), q.id, target, ""); e != nil {
		return e
	}
	q.reserved = target
	return nil
}
func (q *queueBilling) Settle(actual int) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if !q.trace.Resolve(ctx) {
		common.SysError("model billing hold requires reconciliation: " + q.id)
		return errors.New("model outcome is not durable; billing hold retained")
	}
	return model.AdjustQueueHold(ctx, q.id, actual, "settled")
}
func (q *queueBilling) Refund(c *gin.Context) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if !q.trace.Resolve(ctx) {
		common.SysError("model billing hold requires reconciliation: " + q.id)
		return
	}
	if e := model.AdjustQueueHold(ctx, q.id, 0, "released"); e != nil {
		common.SysError("model hold release requires reconciliation: " + q.id)
	}
}
