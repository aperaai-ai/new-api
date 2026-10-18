package asyncprovider

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/QuantumNous/new-api/common"
)

type Tripo struct {
	Key     string
	HTTP    *http.Client
	BaseURL string
}

func (t Tripo) Prepare(r Request, version string) ([]byte, error) {
	switch version {
	case "v3.1-20260211", "v3.0-20250812", "v2.5-20250123":
	default:
		return nil, fmt.Errorf("unsupported Tripo model version")
	}
	return common.Marshal(struct {
		Model          string `json:"model"`
		Prompt         string `json:"prompt,omitempty"`
		Input          string `json:"input,omitempty"`
		Texture        bool   `json:"texture"`
		PBR            bool   `json:"pbr"`
		TextureQuality string `json:"texture_quality"`
	}{version, r.Prompt, r.Input, true, true, "standard"})
}
func (t Tripo) post(ctx context.Context, path string, body []byte, out any) (int, int, error) {
	base := t.BaseURL
	if base == "" {
		base = "https://openapi.tripo3d.ai/v3"
	}
	req, e := http.NewRequestWithContext(ctx, http.MethodPost, base+path, bytes.NewReader(body))
	if e != nil {
		return 0, 0, fmt.Errorf("invalid provider request")
	}
	req.Header.Set("Authorization", "Bearer "+t.Key)
	req.Header.Set("Content-Type", "application/json")
	client := t.HTTP
	if client == nil {
		client = &http.Client{Timeout: 15 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	}
	res, e := client.Do(req)
	if e != nil {
		return 0, 0, fmt.Errorf("provider transport failure")
	}
	defer res.Body.Close()
	retry, _ := strconv.Atoi(res.Header.Get("Retry-After"))
	if retry < 1 {
		retry = 10
	}
	if retry > 300 {
		retry = 300
	}
	raw, e := io.ReadAll(io.LimitReader(res.Body, (2<<20)+1))
	if e != nil || len(raw) > 2<<20 || common.Unmarshal(raw, out) != nil {
		return res.StatusCode, retry, fmt.Errorf("invalid provider response")
	}
	return res.StatusCode, retry, nil
}
func (t Tripo) Submit(ctx context.Context, kind string, body []byte) (SubmitResult, error) {
	var out struct {
		Code *int `json:"code"`
		Data struct {
			ID string `json:"task_id"`
		} `json:"data"`
	}
	path := "/generation/text-to-model"
	if kind == "image_to_model" {
		path = "/generation/image-to-model"
	} else if kind != "text_to_model" {
		return SubmitResult{Rejected: true}, fmt.Errorf("unsupported type")
	}
	status, retry, e := t.post(ctx, path, body, &out)
	if e != nil {
		return SubmitResult{}, e
	}
	if out.Code == nil {
		return SubmitResult{}, fmt.Errorf("provider acknowledgement missing code")
	}
	if status == 429 && (*out.Code == 1007 || *out.Code == 2000) {
		return SubmitResult{Rejected: true, RetryAfter: retry}, fmt.Errorf("provider busy")
	}
	if *out.Code != 0 && (status == 200 || status >= 400 && status < 500) {
		return SubmitResult{Rejected: true}, fmt.Errorf("provider rejected request")
	}
	if status != 200 || *out.Code != 0 || out.Data.ID == "" || len(out.Data.ID) > 191 {
		return SubmitResult{}, fmt.Errorf("unconfirmed provider submission")
	}
	return SubmitResult{ID: out.Data.ID}, nil
}
func (t Tripo) Poll(ctx context.Context, ids []string) (map[string]Result, error) {
	if len(ids) < 1 || len(ids) > 100 {
		return nil, fmt.Errorf("batch size must be 1–100")
	}
	body, e := common.Marshal(map[string]any{"task_ids": ids})
	if e != nil {
		return nil, e
	}
	var out struct {
		Code *int `json:"code"`
		Data struct {
			Tasks map[string]Result `json:"tasks"`
		} `json:"data"`
	}
	status, _, e := t.post(ctx, "/tasks/list", body, &out)
	if e != nil {
		return nil, e
	}
	if status != 200 || out.Code == nil || *out.Code != 0 || out.Data.Tasks == nil {
		return nil, fmt.Errorf("provider query failed")
	}
	return out.Data.Tasks, nil
}
