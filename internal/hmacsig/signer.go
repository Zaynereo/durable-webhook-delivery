package hmacsig

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"time"
)

const HeaderSignature = "X-Webhook-Signature"
const HeaderTimestamp = "X-Webhook-Timestamp"

// Sign returns hex-encoded HMAC-SHA256 over timestamp + "." + body.
func Sign(secret string, timestamp int64, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(strconv.FormatInt(timestamp, 10)))
	mac.Write([]byte{'.'})
	mac.Write(body)
	return hex.EncodeToString(mac.Sum(nil))
}

func NowUnix() int64 {
	return time.Now().UTC().Unix()
}

// Verify checks signature for the given timestamp and body.
func Verify(secret, signature string, timestamp int64, body []byte) bool {
	expected := Sign(secret, timestamp, body)
	return hmac.Equal([]byte(expected), []byte(signature))
}
