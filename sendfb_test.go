package whatsmeow

import (
	"context"
	"errors"
	"testing"

	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/types"
)

func TestHandleFBPHashMismatchInvalidatesParticipantDeviceCaches(t *testing.T) {
	group := types.NewJID("120363000000", types.GroupServer)
	participant := types.NewJID("628111111111", types.DefaultUserServer)
	ownID := types.NewJID("628222222222", types.DefaultUserServer)
	cli := &Client{
		groupCache: map[types.JID]*groupMetaCache{
			group: {},
		},
		userDevicesCache: map[types.JID]deviceCache{
			participant: {devices: []types.JID{{User: participant.User, Server: participant.Server, Device: 1}}},
			ownID:       {devices: []types.JID{{User: ownID.User, Server: ownID.Server, Device: 2}}},
		},
		responseWaiters: make(map[string]chan<- *waBinary.Node),
	}

	devices, err := cli.GetUserDevices(context.Background(), []types.JID{participant})
	if err != nil || len(devices) != 1 {
		t.Fatalf("primed cache lookup failed: devices=%v err=%v", devices, err)
	}

	cli.handleFBPHashMismatch(group, []types.JID{participant, ownID})

	if _, ok := cli.groupCache[group]; ok {
		t.Fatal("group metadata cache was not invalidated")
	}
	if _, ok := cli.userDevicesCache[participant]; ok {
		t.Fatal("participant device cache was not invalidated before retry")
	}
	if _, ok := cli.userDevicesCache[ownID]; ok {
		t.Fatal("own device cache was not invalidated before retry")
	}
	_, err = cli.GetUserDevices(context.Background(), []types.JID{participant})
	if !errors.Is(err, ErrNotConnected) {
		t.Fatalf("same-recipient retry reused stale devices instead of performing a fresh lookup: %v", err)
	}
}
