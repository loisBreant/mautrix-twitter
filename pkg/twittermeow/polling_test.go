package twittermeow

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"go.mau.fi/mautrix-twitter/pkg/twittermeow/cookies"
	"go.mau.fi/mautrix-twitter/pkg/twittermeow/data/payload"
	"go.mau.fi/mautrix-twitter/pkg/twittermeow/data/response"
	"go.mau.fi/mautrix-twitter/pkg/twittermeow/data/types"
)

func assertPollingIntervalsWithin(t *testing.T, pc *PollingClient, minimum, maximum time.Duration) map[time.Duration]struct{} {
	t.Helper()
	seen := make(map[time.Duration]struct{})
	for range 1000 {
		interval := pc.nextInterval()
		if interval < minimum || interval > maximum {
			t.Fatalf("nextInterval() = %s, want between %s and %s", interval, minimum, maximum)
		}
		seen[interval] = struct{}{}
	}
	return seen
}

func TestPollingIntervalDefaultsToJitteredOneToTwoMinutes(t *testing.T) {
	client := NewClient(cookies.NewCookies(nil), nil, zerolog.Nop())
	seen := assertPollingIntervalsWithin(t, client.polling, 60*time.Second, 120*time.Second)
	if len(seen) < 2 {
		t.Fatal("nextInterval() returned the same interval every time, want jitter")
	}
}

func TestSetPollingIntervalUsesConfiguredRange(t *testing.T) {
	client := NewClient(cookies.NewCookies(nil), nil, zerolog.Nop())
	client.SetPollingInterval(5*time.Minute, 10*time.Minute)
	assertPollingIntervalsWithin(t, client.polling, 5*time.Minute, 10*time.Minute)
}

func TestSetPollingIntervalRejectsInvalidRanges(t *testing.T) {
	for _, tc := range []struct {
		name               string
		minimum, maximum   time.Duration
		wantMin, wantMax   time.Duration
		wantSingleInterval bool
	}{
		{name: "unset", wantMin: 60 * time.Second, wantMax: 120 * time.Second},
		{name: "negative", minimum: -time.Second, maximum: -time.Second, wantMin: 60 * time.Second, wantMax: 120 * time.Second},
		{name: "maximum below minimum", minimum: 90 * time.Second, maximum: 30 * time.Second, wantMin: 90 * time.Second, wantMax: 90 * time.Second, wantSingleInterval: true},
		{name: "fixed", minimum: 45 * time.Second, maximum: 45 * time.Second, wantMin: 45 * time.Second, wantMax: 45 * time.Second, wantSingleInterval: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := NewClient(cookies.NewCookies(nil), nil, zerolog.Nop())
			client.SetPollingInterval(tc.minimum, tc.maximum)
			seen := assertPollingIntervalsWithin(t, client.polling, tc.wantMin, tc.wantMax)
			if tc.wantSingleInterval && len(seen) != 1 {
				t.Fatalf("got %d distinct intervals, want 1", len(seen))
			}
		})
	}
}

func TestPollingBackoff(t *testing.T) {
	for _, tc := range []struct {
		failures    int
		rateLimited bool
		want        time.Duration
	}{
		{1, false, 30 * time.Second},
		{2, false, time.Minute},
		{3, false, 2 * time.Minute},
		{4, false, 3 * time.Minute},
		{40, false, 3 * time.Minute},
		{1, true, 2 * time.Minute},
		{2, true, 4 * time.Minute},
		{3, true, 8 * time.Minute},
		{4, true, 15 * time.Minute},
		{100, true, 15 * time.Minute},
	} {
		t.Run(fmt.Sprintf("%d failures, rate limited %t", tc.failures, tc.rateLimited), func(t *testing.T) {
			if got := pollingBackoff(tc.failures, tc.rateLimited); got != tc.want {
				t.Fatalf("pollingBackoff() = %s, want %s", got, tc.want)
			}
		})
	}
}

func TestIsPollingRateLimited(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{"code 88", fmt.Errorf("HTTP 429: %w", &TwitterErrors{Errors: []TwitterError{{Code: 88}}, StatusCode: http.StatusTooManyRequests}), true},
		{"429 without a body", fmt.Errorf("HTTP 429: %w", ErrRatelimitExceeded), true},
		{"429 with another code", fmt.Errorf("HTTP 429: %w", &TwitterErrors{Errors: []TwitterError{{Code: 130}}, StatusCode: http.StatusTooManyRequests}), true},
		{"429 with an empty error list", fmt.Errorf("HTTP 429: %w", &TwitterErrors{StatusCode: http.StatusTooManyRequests}), true},
		{"server error", fmt.Errorf("HTTP 503: %w", &TwitterErrors{Errors: []TwitterError{{Code: 130}}, StatusCode: http.StatusServiceUnavailable}), false},
		{"network error", errors.New("connection reset"), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := isPollingRateLimited(tc.err); got != tc.want {
				t.Fatalf("isPollingRateLimited(%v) = %t, want %t", tc.err, got, tc.want)
			}
		})
	}
}

// newPollingTestClient returns a client whose user_updates requests are answered
// by respond, and a counter of those requests.
func newPollingTestClient(t *testing.T, respond func() *http.Response) (*Client, *atomic.Int32, *atomic.Int32) {
	t.Helper()
	var requests, pollingErrors atomic.Int32
	client := NewClient(cookies.NewCookies(nil), nil, zerolog.Nop())
	client.HTTP = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if !strings.Contains(req.URL.Path, "/dm/user_updates.json") {
			t.Errorf("unexpected request to %s", req.URL)
		}
		requests.Add(1)
		return respond(), nil
	})}
	client.SetEventHandler(func(evt types.TwitterEvent, _ *response.TwitterInboxData) bool {
		if pollErr, ok := evt.(*types.PollingError); ok && pollErr.Error != nil {
			pollingErrors.Add(1)
		}
		return true
	}, nil, func(context.Context) {})
	t.Cleanup(func() {
		client.StopPolling()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := client.polling.pollingStopped.Wait(ctx); err != nil {
			t.Errorf("polling did not stop: %v", err)
		}
	})
	return client, &requests, &pollingErrors
}

func waitForCount(t *testing.T, counter *atomic.Int32, want int32) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for counter.Load() < want {
		if time.Now().After(deadline) {
			t.Fatalf("count = %d, want %d", counter.Load(), want)
		}
		time.Sleep(time.Millisecond)
	}
}

// lockedBuffer collects log output written from the polling goroutine.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func TestPollingBacksOffAfterFailure(t *testing.T) {
	for _, tc := range []struct {
		name      string
		status    int
		body      string
		wantRetry string
	}{
		{"rate limited", http.StatusTooManyRequests, `{"errors":[{"code":88,"message":"Rate limit exceeded"}]}`, `"retry_in":120000`},
		{"other error", http.StatusBadRequest, `{"errors":[{"code":34,"message":"Sorry, that page does not exist"}]}`, `"retry_in":30000`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client, requests, pollingErrors := newPollingTestClient(t, func() *http.Response {
				return &http.Response{
					StatusCode: tc.status,
					Header:     make(http.Header),
					Body:       io.NopCloser(strings.NewReader(tc.body)),
				}
			})
			logs := &lockedBuffer{}
			client.Logger = zerolog.New(logs)
			client.SetPollingInterval(time.Millisecond, time.Millisecond)
			client.StartPolling(context.Background())

			waitForCount(t, pollingErrors, 1)
			// A message request signal during the backoff must not hit the endpoint again.
			client.polling.doShortCircuit()
			time.Sleep(200 * time.Millisecond)
			if got := requests.Load(); got != 1 {
				t.Fatalf("user_updates requests = %d, want 1 while backing off", got)
			}
			if !strings.Contains(logs.String(), tc.wantRetry) {
				t.Fatalf("polling log = %s, want %s", logs.String(), tc.wantRetry)
			}
		})
	}
}

func TestPollingPollsImmediatelyOnShortCircuit(t *testing.T) {
	client, requests, _ := newPollingTestClient(t, func() *http.Response {
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader(`{}`)),
		}
	})
	client.SetPollingInterval(time.Hour, time.Hour)
	client.StartPolling(context.Background())

	time.Sleep(50 * time.Millisecond)
	if got := requests.Load(); got != 0 {
		t.Fatalf("user_updates requests = %d before the first interval, want 0", got)
	}
	client.polling.doShortCircuit()
	waitForCount(t, requests, 1)
	client.polling.doShortCircuit()
	waitForCount(t, requests, 2)
}

func TestMessageRequestInfoInstructionTriggersPoll(t *testing.T) {
	client := NewClient(cookies.NewCookies(nil), nil, zerolog.Nop())
	err := client.xchatProcessor.processInstruction(context.Background(), &payload.MessageInstruction{
		MessageRequestInfoInstruction: &payload.MessageRequestInfoInstruction{},
	})
	if err != nil {
		t.Fatalf("processInstruction() error = %v", err)
	}
	select {
	case <-client.polling.shortCircuit:
	default:
		t.Fatal("MessageRequestInfoInstruction did not request an immediate poll")
	}
}

func TestMessageRequestInfoInstructionDecodesFromThrift(t *testing.T) {
	// Thrift binary: field 9 of MessageInstruction is an (empty) struct, then the
	// stops of the inner and outer structs. Field 9 is the number PR #230 reads
	// from the web client's schema.
	encoded := []byte{0x0c, 0x00, 0x09, 0x00, 0x00}
	var decoded payload.MessageInstruction
	if err := payload.Decode(encoded, &decoded); err != nil {
		t.Fatalf("Decode() error = %v", err)
	}
	if decoded.MessageRequestInfoInstruction == nil {
		t.Fatal("MessageRequestInfoInstruction was not decoded")
	}
}
