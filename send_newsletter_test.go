package whatsmeow

import (
	"testing"

	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
)

func TestPrepareNewsletterEventMessageNodeIncludesCreationMeta(t *testing.T) {
	node, err := prepareNewsletterMessageNode(
		types.NewJID("123456", types.NewsletterServer),
		"event-message-id",
		&waE2E.Message{EventMessage: &waE2E.EventMessage{}},
		"",
		&MessageDebugTimings{},
	)
	if err != nil {
		t.Fatalf("prepareNewsletterMessageNode failed: %v", err)
	}
	if got := node.Attrs["type"]; got != "event" {
		t.Fatalf("message type = %v, want event", got)
	}

	children := node.GetChildren()
	if len(children) != 2 {
		t.Fatalf("message children = %d, want plaintext and event meta", len(children))
	}
	meta := children[1]
	if meta.Tag != "meta" || meta.Attrs["event_type"] != "creation" {
		t.Fatalf("event meta = %+v, want event_type=creation", meta)
	}
}

func TestPrepareNewsletterTextMessageNodeKeepsPlaintextOnly(t *testing.T) {
	node, err := prepareNewsletterMessageNode(
		types.NewJID("123456", types.NewsletterServer),
		"text-message-id",
		&waE2E.Message{Conversation: stringPointer("hello")},
		"",
		&MessageDebugTimings{},
	)
	if err != nil {
		t.Fatalf("prepareNewsletterMessageNode failed: %v", err)
	}
	children := node.GetChildren()
	if len(children) != 1 || children[0].Tag != "plaintext" {
		t.Fatalf("text message children = %+v, want plaintext only", children)
	}
}

func stringPointer(value string) *string {
	return &value
}
