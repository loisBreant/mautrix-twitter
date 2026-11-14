package twittermeow

import (
	"context"
	"errors"
	"math/rand/v2"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
	"go.mau.fi/util/exsync"

	"go.mau.fi/mautrix-twitter/pkg/twittermeow/data/payload"
	"go.mau.fi/mautrix-twitter/pkg/twittermeow/data/types"
)

const (
	defaultMinPollingInterval = 60 * time.Second
	defaultMaxPollingInterval = 120 * time.Second

	pollingErrorBackoff        = 30 * time.Second
	maxPollingErrorBackoff     = 3 * time.Minute
	pollingRateLimitBackoff    = 2 * time.Minute
	maxPollingRateLimitBackoff = 15 * time.Minute
)

// PollingClient handles REST API polling for DM updates.
// Used primarily for untrusted (message request) conversations
// which don't receive real-time updates via XChat WebSocket.
type PollingClient struct {
	client                *Client
	stop                  atomic.Pointer[context.CancelFunc]
	activeConversationID  string
	includeConversationID bool
	shortCircuit          chan struct{}
	pollingStopped        *exsync.Event
	minInterval           time.Duration
	maxInterval           time.Duration
}

// newPollingClient creates a new polling client.
func (c *Client) newPollingClient() *PollingClient {
	pc := &PollingClient{
		client:         c,
		shortCircuit:   make(chan struct{}, 1),
		pollingStopped: exsync.NewEvent(),
		minInterval:    defaultMinPollingInterval,
		maxInterval:    defaultMaxPollingInterval,
	}
	pc.pollingStopped.Set()
	return pc
}

// setInterval sets the range the wait between two polls is drawn from. A minimum
// of zero or less keeps the default range, and a maximum below the minimum polls
// at the minimum.
func (pc *PollingClient) setInterval(minimum, maximum time.Duration) {
	if minimum <= 0 {
		pc.minInterval, pc.maxInterval = defaultMinPollingInterval, defaultMaxPollingInterval
		return
	}
	pc.minInterval, pc.maxInterval = minimum, maximum
}

// nextInterval draws the wait before the next poll, so that polls don't come at
// a fixed rhythm.
func (pc *PollingClient) nextInterval() time.Duration {
	if pc.maxInterval <= pc.minInterval {
		return pc.minInterval
	}
	return pc.minInterval + rand.N(pc.maxInterval-pc.minInterval+1)
}

// pollingBackoff returns the wait after the given number of consecutive failed
// polls. A rate limit waits longer, up to the 15 minute window X counts requests in.
func pollingBackoff(failures int, rateLimited bool) time.Duration {
	base, ceiling := pollingErrorBackoff, maxPollingErrorBackoff
	if rateLimited {
		base, ceiling = pollingRateLimitBackoff, maxPollingRateLimitBackoff
	}
	backoff := base
	for i := 1; i < failures && backoff < ceiling; i++ {
		backoff *= 2
	}
	return min(backoff, ceiling)
}

// isPollingRateLimited reports whether a poll failed because X rate limited it:
// error code 88, or HTTP 429 whatever its body says.
func isPollingRateLimited(err error) bool {
	var twitterErrs *TwitterErrors
	return errors.Is(err, ErrRatelimitExceeded) ||
		errors.As(err, &twitterErrs) && twitterErrs.StatusCode == http.StatusTooManyRequests
}

func (pc *PollingClient) startPolling(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	if oldCancel := pc.stop.Swap(&cancel); oldCancel != nil {
		(*oldCancel)()
	}
	pc.pollingStopped.Clear()
	go func() {
		defer func() {
			if pc.stop.Load() == &cancel {
				pc.pollingStopped.Set()
			}
		}()
		pc.doPoll(ctx)
	}()
}

func (pc *PollingClient) doPoll(ctx context.Context) {
	timer := time.NewTimer(pc.nextInterval())
	defer timer.Stop()
	log := zerolog.Ctx(ctx)
	failures := 0
	for {
		select {
		case <-timer.C:
		case <-pc.shortCircuit:
			if failures > 0 {
				// Keep waiting out the backoff: polling now could extend a rate limit.
				continue
			}
		case <-ctx.Done():
			log.Debug().Err(ctx.Err()).Msg("Polling context canceled")
			return
		}
		err := pc.poll(ctx)
		if ctx.Err() != nil {
			log.Debug().Err(err).Msg("Polling context canceled during event handling")
			return
		} else if err != nil {
			authError := IsAuthError(err)
			pc.client.eventHandler(&types.PollingError{Error: err, IsAuth: authError}, nil)
			if authError {
				log.Err(err).Msg("Failed to poll for updates")
				return
			}
			failures++
			backoff := pollingBackoff(failures, isPollingRateLimited(err))
			log.Err(err).Dur("retry_in", backoff).Msg("Failed to poll for updates")
			timer.Reset(backoff)
			continue
		} else if failures > 0 {
			failures = 0
			pc.client.eventHandler(&types.PollingError{}, nil)
		}
		timer.Reset(pc.nextInterval())
	}
}

var errEventHandlerFailed = errors.New("event handler failed")

func (pc *PollingClient) poll(ctx context.Context) error {
	userUpdatesQuery := (&payload.DMRequestQuery{}).Default()
	// Include low quality / untrusted conversations (message requests)
	userUpdatesQuery.FilterLowQuality = false
	if pc.includeConversationID {
		userUpdatesQuery.ActiveConversationID = pc.activeConversationID
	} else {
		userUpdatesQuery.ActiveConversationID = ""
	}
	pc.includeConversationID = !pc.includeConversationID
	userUpdatesQuery.Cursor = pc.client.session.PollingCursor

	userUpdatesResponse, err := pc.client.GetDMUserUpdates(ctx, &userUpdatesQuery)
	if err != nil {
		return err
	}

	// UserEvents can be nil if there are no updates
	if userUpdatesResponse.UserEvents == nil {
		return nil
	}

	if !pc.client.eventHandler(nil, userUpdatesResponse.UserEvents) {
		return errEventHandlerFailed
	}
	for _, entry := range userUpdatesResponse.UserEvents.Entries {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		parsed := entry.ParseWithErrorLog(&pc.client.Logger)
		if parsed != nil {
			if !pc.client.eventHandler(parsed, userUpdatesResponse.UserEvents) {
				return errEventHandlerFailed
			}
		}
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if userUpdatesResponse.UserEvents.Cursor != pc.client.session.PollingCursor {
		pc.client.session.PollingCursor = userUpdatesResponse.UserEvents.Cursor
		pc.client.onCursorChanged(ctx)
	}
	return nil
}

func (pc *PollingClient) stopPolling() {
	if cancel := pc.stop.Load(); cancel != nil {
		(*cancel)()
	}
	pc.activeConversationID = ""
}

// SetActiveConversation sets the active conversation and triggers a short-circuit poll.
func (pc *PollingClient) SetActiveConversation(conversationID string) {
	pc.activeConversationID = conversationID
	pc.pollConversation(conversationID)
}

func (pc *PollingClient) doShortCircuit() {
	select {
	case pc.shortCircuit <- struct{}{}:
	default:
	}
}

func (pc *PollingClient) pollConversation(conversationID string) {
	if pc.activeConversationID == conversationID {
		pc.includeConversationID = true
		pc.doShortCircuit()
	}
}
