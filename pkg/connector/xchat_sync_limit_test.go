package connector

import (
	"context"
	"fmt"
	"testing"

	"go.mau.fi/mautrix-twitter/pkg/twittermeow/data/payload"
	"go.mau.fi/mautrix-twitter/pkg/twittermeow/data/response"
)

// xchatLimitTestInbox serves an inbox of pages that never ends, so a test sees
// how many pages the import asks for before it stops on its own.
type xchatLimitTestInbox struct {
	t           *testing.T
	pageSize    int
	pages       int
	lastPage    int // 0: endless
	checkpoints []xchatCheckpointRecord
}

func (inbox *xchatLimitTestInbox) page() response.XChatInboxPage {
	inbox.pages++
	page := response.XChatInboxPage{
		Items:             make([]response.XChatInboxItem, inbox.pageSize),
		MaxUserSequenceID: xchatTestString(fmt.Sprintf("%d", 1000+inbox.pages)),
	}
	if inbox.lastPage != 0 && inbox.pages >= inbox.lastPage {
		page.InboxCursor = response.XChatInboxCursor{PullFinished: true}
	} else {
		page.InboxCursor = response.XChatInboxCursor{
			CursorID:        fmt.Sprintf("cursor-%d", inbox.pages),
			GraphSnapshotID: "graph",
		}
	}
	return page
}

func (inbox *xchatLimitTestInbox) ops() xchatInboxCatchupOps {
	return xchatInboxCatchupOps{
		FetchInitial: func(context.Context, *payload.GetInitialXChatPageQueryVariables) (response.XChatInboxPage, error) {
			return inbox.page(), nil
		},
		FetchNext: func(context.Context, *payload.GetInboxPageRequestQueryVariables) (response.XChatInboxPage, error) {
			if inbox.pages >= 50 {
				inbox.t.Fatal("import kept paging past the conversation limit")
			}
			return inbox.page(), nil
		},
		ProcessPage: func(context.Context, response.XChatInboxPage) (xchatInboxPageProcessResult, error) {
			return xchatInboxPageProcessResult{}, nil
		},
		Checkpoint: func(_ context.Context, cursor *payload.XChatCursor, maxSequence string, pullVersion *int) error {
			inbox.checkpoints = append(inbox.checkpoints, xchatCheckpointRecord{
				cursor:      cloneXChatCursor(cursor),
				maxSequence: maxSequence,
				pullVersion: cloneXChatInt(pullVersion),
			})
			return nil
		},
	}
}

func (inbox *xchatLimitTestInbox) finalCheckpoint() xchatCheckpointRecord {
	inbox.t.Helper()
	if len(inbox.checkpoints) == 0 {
		inbox.t.Fatal("import saved no checkpoint")
	}
	return inbox.checkpoints[len(inbox.checkpoints)-1]
}

func TestRunXChatInboxCatchupStopsAfterInitialPageAtConversationLimit(t *testing.T) {
	inbox := &xchatLimitTestInbox{t: t, pageSize: 20}
	result, err := runXChatInboxCatchup(context.Background(), xchatInboxCatchupState{ConversationLimit: 20}, inbox.ops())
	if err != nil {
		t.Fatalf("runXChatInboxCatchup() error = %v", err)
	}
	if inbox.pages != 1 {
		t.Fatalf("fetched %d pages, want 1", inbox.pages)
	}
	if result.Conversations != 20 {
		t.Fatalf("result conversations = %d, want 20", result.Conversations)
	}
	// The import is finished: no cursor is left for a reconnect to resume, and the
	// sequence moves to the inbox's, so the next connect only catches up from here.
	final := inbox.finalCheckpoint()
	if final.cursor != nil {
		t.Fatalf("final checkpoint cursor = %#v, want nil", final.cursor)
	}
	if final.maxSequence != "1001" || result.MaxSequenceID != "1001" {
		t.Fatalf("final max sequence = %q (result %q), want 1001", final.maxSequence, result.MaxSequenceID)
	}
}

func TestRunXChatInboxCatchupStopsOnThePageThatReachesConversationLimit(t *testing.T) {
	inbox := &xchatLimitTestInbox{t: t, pageSize: 20}
	result, err := runXChatInboxCatchup(context.Background(), xchatInboxCatchupState{ConversationLimit: 25}, inbox.ops())
	if err != nil {
		t.Fatalf("runXChatInboxCatchup() error = %v", err)
	}
	if inbox.pages != 2 || result.Conversations != 40 {
		t.Fatalf("fetched %d pages with %d conversations, want 2 pages with 40", inbox.pages, result.Conversations)
	}
	if final := inbox.finalCheckpoint(); final.cursor != nil || final.maxSequence != "1002" {
		t.Fatalf("final checkpoint = %#v, want no cursor at sequence 1002", final)
	}
}

func TestRunXChatInboxCatchupWithoutConversationLimitImportsEveryPage(t *testing.T) {
	for _, limit := range []int{0, -1} {
		t.Run(fmt.Sprintf("limit %d", limit), func(t *testing.T) {
			inbox := &xchatLimitTestInbox{t: t, pageSize: 20, lastPage: 4}
			result, err := runXChatInboxCatchup(context.Background(), xchatInboxCatchupState{ConversationLimit: limit}, inbox.ops())
			if err != nil {
				t.Fatalf("runXChatInboxCatchup() error = %v", err)
			}
			if inbox.pages != 4 || result.Conversations != 80 {
				t.Fatalf("fetched %d pages with %d conversations, want 4 pages with 80", inbox.pages, result.Conversations)
			}
		})
	}
}

func TestRunXChatInboxCatchupConversationLimitCountsEarlierAttempts(t *testing.T) {
	// A retry after a failed page resumes from the saved cursor. Conversations the
	// failed attempt already imported still count against the limit.
	inbox := &xchatLimitTestInbox{t: t, pageSize: 5}
	result, err := runXChatInboxCatchup(context.Background(), xchatInboxCatchupState{
		MaxSequenceID:     "900",
		Cursor:            &payload.XChatCursor{CursorId: "resume", GraphSnapshotId: "graph"},
		ConversationLimit: 20,
		Conversations:     15,
	}, inbox.ops())
	if err != nil {
		t.Fatalf("runXChatInboxCatchup() error = %v", err)
	}
	if inbox.pages != 1 || result.Conversations != 20 {
		t.Fatalf("fetched %d pages with %d conversations in total, want 1 page reaching 20", inbox.pages, result.Conversations)
	}
	if final := inbox.finalCheckpoint(); final.cursor != nil {
		t.Fatalf("final checkpoint cursor = %#v, want nil", final.cursor)
	}
}

func TestRunXChatInboxCatchupReportsConversationsOfAFailedAttempt(t *testing.T) {
	inbox := &xchatLimitTestInbox{t: t, pageSize: 20}
	ops := inbox.ops()
	ops.FetchNext = func(context.Context, *payload.GetInboxPageRequestQueryVariables) (response.XChatInboxPage, error) {
		return response.XChatInboxPage{}, fmt.Errorf("rate limited")
	}
	result, err := runXChatInboxCatchup(context.Background(), xchatInboxCatchupState{ConversationLimit: 100}, ops)
	if err == nil {
		t.Fatal("runXChatInboxCatchup() error = nil, want the fetch error")
	}
	if result.Conversations != 20 {
		t.Fatalf("result conversations = %d, want the 20 imported before the failure", result.Conversations)
	}
}

func TestXChatImportConversationLimitAppliesToFreshImportsOnly(t *testing.T) {
	for _, tc := range []struct {
		name string
		meta *UserLoginMetadata
		want int
	}{
		{"first login", &UserLoginMetadata{}, 20},
		{"reconnect", &UserLoginMetadata{MaxUserSequenceID: "100"}, 0},
		{"interrupted import", &UserLoginMetadata{XChatInboxCursor: &XChatInboxCursorData{CursorID: "c", GraphSnapshotID: "g"}}, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := xchatImportConversationLimit(tc.meta, 20); got != tc.want {
				t.Fatalf("xchatImportConversationLimit() = %d, want %d", got, tc.want)
			}
		})
	}
}
