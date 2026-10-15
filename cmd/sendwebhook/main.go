// Command sendwebhook posts a signed provider webhook, for local testing:
//
//	go run ./cmd/sendwebhook -payment <id> -type payment.succeeded
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/unsound-mint/payhook/internal/webhook"
)

func main() {
	url := flag.String("url", "http://localhost:8080/v1/webhooks/halyk", "webhook endpoint")
	secret := flag.String("secret", "whsec_local_halyk", "provider signing secret")
	paymentID := flag.String("payment", "", "payment id (required)")
	typ := flag.String("type", "payment.succeeded", "event type")
	eventID := flag.String("event", fmt.Sprintf("evt_%d", time.Now().UnixNano()), "provider event id")
	flag.Parse()
	if *paymentID == "" {
		flag.Usage()
		os.Exit(2)
	}

	body, _ := json.Marshal(map[string]string{"id": *eventID, "type": *typ, "payment_id": *paymentID})
	req, _ := http.NewRequest(http.MethodPost, *url, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Signature", webhook.Sign([]byte(*secret), time.Now(), body))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer resp.Body.Close()
	out, _ := io.ReadAll(resp.Body)
	fmt.Printf("%d %s", resp.StatusCode, out)
}
