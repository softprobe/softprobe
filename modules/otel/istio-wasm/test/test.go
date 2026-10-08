package main

import (
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

func mustGetEnv(key, def string) string {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	return v
}

func main() {
	rand.Seed(time.Now().UnixNano())
	backendURL := mustGetEnv("BACKEND_URL", "https://o.softprobe.ai")
	serviceName := mustGetEnv("SERVICE_NAME", "softprobe-integration-test")
	sessionID := fmt.Sprintf("session-%d", time.Now().Unix())
	testID := fmt.Sprintf("test-%d", rand.Intn(1_000_000))

	// Inside compose: talk to envoy by service DNS
	envoyHost := mustGetEnv("ENVOY_HOST", "envoy")
	inboundBase := fmt.Sprintf("http://%s:15006", envoyHost)
	adminBase := fmt.Sprintf("http://%s:18001", envoyHost)

	// Softprobe backend config

	// Record start time before sending traffic
	testStart := time.Now().UTC().Format(time.RFC3339)

	client := &http.Client{Timeout: 20 * time.Second}

	// 1) GET /json via inbound listener -> go-app -> httpbin via outbound
	req1, _ := http.NewRequest(http.MethodGet, inboundBase+"/json", nil)
	req1.Header.Set("X-Session-ID", sessionID)
	req1.Header.Set("X-Test-Request-ID", testID)
	resp1, err := client.Do(req1)
	if err != nil {
		panic(err)
	}
	if resp1.StatusCode/100 != 2 {
		panic(fmt.Sprintf("/json status=%d", resp1.StatusCode))
	}
	body1, _ := io.ReadAll(resp1.Body)
	resp1.Body.Close()
	var js map[string]any
	_ = json.Unmarshal(body1, &js)

	// 2) POST /delay/2
	req2, _ := http.NewRequest(http.MethodPost, inboundBase+"/delay/2", strings.NewReader("demo"))
	req2.Header.Set("Content-Type", "text/plain")
	req2.Header.Set("X-Session-ID", sessionID)
	req2.Header.Set("X-Test-Request-ID", testID)
	resp2, err := client.Do(req2)
	if err != nil {
		panic(err)
	}
	if resp2.StatusCode/100 != 2 {
		panic(fmt.Sprintf("/delay status=%d", resp2.StatusCode))
	}
	io.Copy(io.Discard, resp2.Body)
	resp2.Body.Close()

	// 3) Optional: check admin
	_, _ = client.Get(adminBase + "/stats")

	// Build Softprobe query URLs (print for manual curl validation)
	q := url.Values{}
	q.Set("serviceName", serviceName)
	q.Set("startTimeFrom", testStart)
	q.Set("size", "10")
	sessionURL := fmt.Sprintf("%s/api/tenants/test-with-userid-v3/sessions?%s", strings.TrimRight(backendURL, "/"), q.Encode())
	tracesEndpoint := fmt.Sprintf("%s/api/tenants/test-with-userid-v3/sessions/%s", strings.TrimRight(backendURL, "/"), url.PathEscape(sessionID))
	fmt.Println("Softprobe traces URL:", tracesEndpoint)
	fmt.Println("Softprobe session URL:", sessionURL)
	fmt.Println("Suggested curl (JSON): curl -s -H 'Accept: application/json' '" + tracesEndpoint + "' | jq .")
	fmt.Println("Suggested curl (JSON): curl -s -H 'Accept: application/json' '" + sessionURL + "' | jq .")

	// Poll traces by service
	found := false
	for i := 0; i < 3; i++ { // up to ~15s
		time.Sleep(5 * time.Second)
		req3, _ := http.NewRequest(http.MethodGet, tracesEndpoint, nil)
		req3.Header.Set("Accept", "application/json")
		resp3, err := client.Do(req3)
		if err == nil {
			body3, _ := io.ReadAll(resp3.Body)
			resp3.Body.Close()
			if resp3.StatusCode/100 == 2 {
				var tracesResp struct {
					Traces []any `json:"traces"`
				}
				_ = json.Unmarshal(body3, &tracesResp)
				if len(tracesResp.Traces) > 0 {
					found = true
					break
				}
			}
		}
	}
	if !found {
		panic("no traces found in Softprobe backend for service during test window")
	}

	// Poll session traces
	sessFound := false
	for i := 0; i < 3; i++ { // up to ~15s
		time.Sleep(5 * time.Second)
		req4, _ := http.NewRequest(http.MethodGet, sessionURL, nil)
		req4.Header.Set("Accept", "application/json")
		resp4, err := client.Do(req4)
		if err == nil {
			body4, _ := io.ReadAll(resp4.Body)
			resp4.Body.Close()
			if resp4.StatusCode/100 == 2 {
				var ses struct {
					TotalCount int `json:"totalCount"`
				}
				_ = json.Unmarshal(body4, &ses)
				if ses.TotalCount > 0 {
					sessFound = true
					break
				}
			}
		}
	}
	if !sessFound {
		panic("no session traces found for test session")
	}

	fmt.Println("OK")
}
