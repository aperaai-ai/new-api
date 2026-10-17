package asyncprovider

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/QuantumNous/new-api/common"
	"github.com/stretchr/testify/require"
)

type transport func(*http.Request) (*http.Response, error)

func (f transport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func mockClient(status int, body string, check func(*http.Request)) *http.Client {
	return &http.Client{Transport: transport(func(r *http.Request) (*http.Response, error) {
		if check != nil {
			check(r)
		}
		return &http.Response{StatusCode: status, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(body))}, nil
	})}
}
func TestTripoProtocol(t *testing.T) {
	provider := Tripo{Key: "secret", HTTP: mockClient(200, `{"code":0,"data":{"task_id":"task_123"}}`, func(r *http.Request) {
		require.Equal(t, "/v3/generation/text-to-model", r.URL.Path)
		require.Equal(t, "Bearer secret", r.Header.Get("Authorization"))
	})}
	req, e := Parse([]byte(`{"model":"tripo-standard","type":"text_to_model","prompt":"a cat"}`))
	require.NoError(t, e)
	body, e := provider.Prepare(req, "v3.1-20260211")
	require.NoError(t, e)
	var payload map[string]any
	require.NoError(t, common.Unmarshal(body, &payload))
	require.Equal(t, true, payload["pbr"])
	result, e := provider.Submit(context.Background(), req.Type, body)
	require.NoError(t, e)
	require.Equal(t, "task_123", result.ID)
}
func TestSubmissionClassification(t *testing.T) {
	cases := []struct {
		body           string
		status         int
		rejected, busy bool
	}{
		{`{"data":{"task_id":"task_123"}}`, 200, false, false},
		{`upstream timed out`, 504, false, false},
		{`{"code":0,"data":{}}`, 200, false, false},
		{`{"code":1004}`, 400, true, false},
		{`{"code":2000}`, 429, true, true},
		{`{"code":9999}`, 500, false, false},
	}
	for _, c := range cases {
		p := Tripo{HTTP: mockClient(c.status, c.body, nil)}
		r, e := p.Submit(context.Background(), "text_to_model", []byte(`{}`))
		require.Error(t, e)
		require.Equal(t, c.rejected, r.Rejected)
		require.Equal(t, c.busy, r.RetryAfter > 0)
	}
}
func TestBatchAndMissingAreNotFailure(t *testing.T) {
	p := Tripo{HTTP: mockClient(200, `{"code":0,"data":{"tasks":{"task_a":{"task_id":"task_a","status":"running","progress":30}},"missed":["task_b"]}}`, nil)}
	out, e := p.Poll(context.Background(), []string{"task_a", "task_b"})
	require.NoError(t, e)
	require.Equal(t, "running", out["task_a"].Status)
	_, exists := out["task_b"]
	require.False(t, exists)
	_, e = p.Poll(context.Background(), make([]string, 101))
	require.Error(t, e)
}
func TestValidationAndCredentialBinding(t *testing.T) {
	for _, body := range []string{`null`, `{"model":"m","type":"image_to_model","input":"http://example.com/a.png"}`, `{"model":"m","type":"text_to_model","prompt":"a","texture_quality":"extreme"}`} {
		_, e := Parse([]byte(body))
		require.Error(t, e)
	}
	encrypted, e := Seal("stable-key", "job-a", "provider-secret")
	require.NoError(t, e)
	plain, e := Open("stable-key", "job-a", encrypted)
	require.NoError(t, e)
	require.Equal(t, "provider-secret", plain)
	_, e = Open("stable-key", "job-b", encrypted)
	require.Error(t, e)
	_, e = Open("wrong-key", "job-a", encrypted)
	require.Error(t, e)
}
