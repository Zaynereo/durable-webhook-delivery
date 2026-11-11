package destination_test

import (
	"errors"
	"testing"

	"github.com/zaynereo/durable-webhook-delivery/internal/destination"
)

func TestAllowlistValidate(t *testing.T) {
	al, err := destination.NewAllowlist([]string{
		"http://sink:9090/webhook",
		"http://127.0.0.1:9090/webhook",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := al.Validate("http://sink:9090/webhook"); err != nil {
		t.Fatalf("expected allowed: %v", err)
	}
	if err := al.Validate("http://evil.example/hook"); !errors.Is(err, destination.ErrDestinationNotAllowed) {
		t.Fatalf("expected not allowed, got %v", err)
	}
	if err := al.Validate("ftp://sink:9090/webhook"); err == nil {
		t.Fatal("expected scheme error")
	}
}
