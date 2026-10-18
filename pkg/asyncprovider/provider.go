// Package asyncprovider defines provider-neutral asynchronous generation contracts.
package asyncprovider

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"strings"
	"unicode/utf8"

	"github.com/QuantumNous/new-api/common"
)

type Request struct {
	Model  string `json:"model"`
	Type   string `json:"type"`
	Prompt string `json:"prompt,omitempty"`
	Input  string `json:"input,omitempty"`
}

func Parse(body []byte) (Request, error) {
	var r Request
	var fields map[string]json.RawMessage
	if common.Unmarshal(body, &fields) != nil || fields == nil {
		return r, fmt.Errorf("JSON object required")
	}
	for k := range fields {
		if k != "model" && k != "type" && k != "prompt" && k != "input" {
			return r, fmt.Errorf("unsupported parameter: %s", k)
		}
	}
	if common.Unmarshal(body, &r) != nil || len(r.Model) == 0 || len(r.Model) > 128 {
		return r, fmt.Errorf("invalid model or parameters")
	}
	switch r.Type {
	case "text_to_model":
		if strings.TrimSpace(r.Prompt) == "" || utf8.RuneCountInString(r.Prompt) > 1024 || r.Input != "" {
			return r, fmt.Errorf("prompt must contain 1–1024 characters; input must be absent")
		}
	case "image_to_model":
		if !HTTPS(r.Input) || len(r.Input) > 2048 || r.Prompt != "" {
			return r, fmt.Errorf("input must be a public HTTPS image URL; prompt must be absent")
		}
	default:
		return r, fmt.Errorf("unsupported generation type")
	}
	return r, nil
}
func HTTPS(s string) bool {
	u, e := url.Parse(s)
	return e == nil && u.Scheme == "https" && u.Hostname() != "" && u.User == nil
}

type Result struct {
	ID       string      `json:"task_id"`
	Status   string      `json:"status"`
	Progress int         `json:"progress"`
	Credits  json.Number `json:"credits_consumed,omitempty"`
	Output   struct {
		ModelURL   string `json:"model_url,omitempty"`
		PreviewURL string `json:"rendered_image_url,omitempty"`
	} `json:"output"`
}

// Rejected is only true when an upstream response confirms no job was accepted.
// All other submission errors require reconciliation, never automatic resubmission.
type SubmitResult struct {
	ID         string
	Rejected   bool
	RetryAfter int
}
type Provider interface {
	Prepare(Request, string) ([]byte, error)
	Submit(context.Context, string, []byte) (SubmitResult, error)
	Poll(context.Context, []string) (map[string]Result, error)
}

func Seal(secret, aad, plaintext string) (string, error) {
	key := sha256.Sum256([]byte(secret))
	b, e := aes.NewCipher(key[:])
	if e != nil {
		return "", e
	}
	g, e := cipher.NewGCM(b)
	if e != nil {
		return "", e
	}
	n := make([]byte, g.NonceSize())
	if _, e = io.ReadFull(rand.Reader, n); e != nil {
		return "", e
	}
	return base64.StdEncoding.EncodeToString(g.Seal(n, n, []byte(plaintext), []byte(aad))), nil
}
func Open(secret, aad, encoded string) (string, error) {
	key := sha256.Sum256([]byte(secret))
	b, _ := aes.NewCipher(key[:])
	g, _ := cipher.NewGCM(b)
	raw, e := base64.StdEncoding.DecodeString(encoded)
	if e != nil || len(raw) < g.NonceSize() {
		return "", fmt.Errorf("invalid credential snapshot")
	}
	p, e := g.Open(nil, raw[:g.NonceSize()], raw[g.NonceSize():], []byte(aad))
	if e != nil {
		return "", fmt.Errorf("credential snapshot authentication failed")
	}
	return string(p), nil
}
