// Copyright 2026 Alibaba Group Holding Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Regression tests for streaming (SSE) connection handling:
//  1. A long-running stream must NOT be killed by the client's overall request
//     timeout (which would kill long commands mid-stream).
//  2. SSE must use a fresh, non-pooled connection (keep-alive disabled), so a
//     silently-dropped idle connection can never stall a stream.

package opensandbox

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// TestStreaming_NotKilledByRequestTimeout: an SSE stream whose events span
// longer than the client's overall RequestTimeout still completes, because
// streaming uses the dedicated timeout-free client. Pre-fix (SSE shared the
// httpClient whose Timeout == RequestTimeout) this failed with
// "Client.Timeout ... while reading body".
func TestStreaming_NotKilledByRequestTimeout(t *testing.T) {
	// Stream 5 events, ~120ms apart => ~600ms total, well beyond the 200ms timeout.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		fl, _ := w.(http.Flusher)
		for i := 0; i < 5; i++ {
			_, _ = w.Write([]byte(`{"type":"metrics","cpu_count":1,"timestamp":` + time.Now().Format("05") + "}\n\n"))
			if fl != nil {
				fl.Flush()
			}
			time.Sleep(120 * time.Millisecond)
		}
	}))
	defer srv.Close()

	// RequestTimeout is short (200ms). Non-streaming would be capped at 200ms,
	// but streaming must not be.
	client := NewExecdClient(srv.URL, "tok", WithTimeout(200*time.Millisecond))

	start := time.Now()
	var events []StreamEvent
	err := client.WatchMetrics(context.Background(), func(e StreamEvent) error {
		events = append(events, e)
		return nil
	})
	elapsed := time.Since(start)
	require.NoError(t, err, "long SSE stream must not be killed by the 200ms overall timeout")
	require.True(t, elapsed > 200*time.Millisecond, "stream should have run longer than the overall timeout")
	require.True(t, len(events) >= 3, "should have received the streamed events")
}

// TestStreaming_UsesNonPooledClient verifies the streaming client disables
// keep-alive (no connection pooling) and has no overall timeout, so streams are
// never carried over a stale pooled connection nor capped by RequestTimeout.
func TestStreaming_UsesNonPooledClient(t *testing.T) {
	client := NewExecdClient("https://example.com", "tok", WithTimeout(30*time.Second))
	sc := client.client.streamHTTPClient()
	require.Equal(t, time.Duration(0), sc.Timeout, "streaming client must have no overall timeout")
	tr, ok := sc.Transport.(*http.Transport)
	require.True(t, ok, "streaming transport should be *http.Transport")
	require.True(t, tr.DisableKeepAlives, "streaming client must disable keep-alive (no connection pooling)")

	// The normal (non-streaming) client keeps its overall timeout and pooling.
	require.Equal(t, 30*time.Second, client.client.httpClient.Timeout, "non-streaming client keeps its request timeout")
}
