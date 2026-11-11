package api_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/zaynereo/durable-webhook-delivery/internal/api"
	"github.com/zaynereo/durable-webhook-delivery/internal/destination"
	"github.com/zaynereo/durable-webhook-delivery/internal/testutil"
)

func TestAPIEnqueueAndStatus(t *testing.T) {
	st := testutil.NewStore(t)
	al, err := destination.NewAllowlist([]string{"http://sink:9090/webhook"})
	if err != nil {
		t.Fatal(err)
	}
	srv := api.NewServer(st, al)

	body := `{"destination_url":"http://sink:9090/webhook","payload":{"event_id":"x1"}}`
	req := httptest.NewRequest(http.MethodPost, "/v1/webhooks", bytes.NewBufferString(body))
	req.Header.Set("Idempotency-Key", "idem-1")
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusAccepted {
		t.Fatalf("status %d body %s", rr.Code, rr.Body.String())
	}
	var resp map[string]any
	_ = json.Unmarshal(rr.Body.Bytes(), &resp)
	id, _ := resp["id"].(string)

	req2 := httptest.NewRequest(http.MethodPost, "/v1/webhooks", bytes.NewBufferString(body))
	req2.Header.Set("Idempotency-Key", "idem-1")
	rr2 := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr2, req2)
	if rr2.Code != http.StatusOK {
		t.Fatalf("duplicate status %d", rr2.Code)
	}

	req3 := httptest.NewRequest(http.MethodGet, "/v1/webhooks/"+id, nil)
	rr3 := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr3, req3)
	if rr3.Code != http.StatusOK {
		t.Fatalf("get status %d", rr3.Code)
	}

	bad := `{"destination_url":"http://evil.example/x","payload":{"a":1}}`
	req4 := httptest.NewRequest(http.MethodPost, "/v1/webhooks", bytes.NewBufferString(bad))
	req4.Header.Set("Idempotency-Key", "idem-2")
	rr4 := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr4, req4)
	if rr4.Code != http.StatusBadRequest {
		t.Fatalf("expected bad request for ssrf, got %d", rr4.Code)
	}
}
