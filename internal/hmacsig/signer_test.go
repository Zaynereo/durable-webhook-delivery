package hmacsig_test

import (
	"testing"

	"github.com/zaynereo/durable-webhook-delivery/internal/hmacsig"
)

func TestSignVerify(t *testing.T) {
	secret := "test-secret"
	body := []byte(`{"event_id":"abc"}`)
	ts := int64(1700000000)
	sig := hmacsig.Sign(secret, ts, body)
	if !hmacsig.Verify(secret, sig, ts, body) {
		t.Fatal("verify failed")
	}
	if hmacsig.Verify(secret, sig, ts, []byte(`{"x":1}`)) {
		t.Fatal("expected mismatch")
	}
}
