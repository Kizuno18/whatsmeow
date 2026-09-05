// Copyright (c) 2026 Tulir Asokan
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package whatsmeow

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"
	"testing"

	"go.mau.fi/libsignal/ecc"
	"go.mau.fi/libsignal/keys/identity"
	"go.mau.fi/libsignal/keys/prekey"
	"go.mau.fi/libsignal/session"
	"go.mau.fi/libsignal/util/optional"

	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/util/keys"
	waLog "go.mau.fi/whatsmeow/util/log"
)

type memorySessionStore struct {
	lock     sync.Mutex
	sessions map[string][]byte
}

func (m *memorySessionStore) GetSession(_ context.Context, address string) ([]byte, error) {
	m.lock.Lock()
	defer m.lock.Unlock()
	return m.sessions[address], nil
}

func (m *memorySessionStore) HasSession(_ context.Context, address string) (bool, error) {
	m.lock.Lock()
	defer m.lock.Unlock()
	_, ok := m.sessions[address]
	return ok, nil
}

func (m *memorySessionStore) GetManySessions(_ context.Context, addresses []string) (map[string][]byte, error) {
	m.lock.Lock()
	defer m.lock.Unlock()
	res := make(map[string][]byte, len(addresses))
	for _, addr := range addresses {
		res[addr] = m.sessions[addr]
	}
	return res, nil
}

func (m *memorySessionStore) PutSession(_ context.Context, address string, session []byte) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.sessions[address] = session
	return nil
}

func (m *memorySessionStore) PutManySessions(_ context.Context, sessions map[string][]byte) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	maps.Copy(m.sessions, sessions)
	return nil
}

func (m *memorySessionStore) DeleteAllSessions(context.Context, string) error { return nil }

func (m *memorySessionStore) DeleteSession(_ context.Context, address string) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	delete(m.sessions, address)
	return nil
}

func (m *memorySessionStore) MigratePNToLID(context.Context, types.JID, types.JID) error { return nil }

type memoryIdentityStore struct {
	lock       sync.Mutex
	identities map[string][32]byte
}

func (m *memoryIdentityStore) PutIdentity(_ context.Context, address string, key [32]byte) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.identities[address] = key
	return nil
}

func (m *memoryIdentityStore) DeleteAllIdentities(context.Context, string) error { return nil }

func (m *memoryIdentityStore) DeleteIdentity(_ context.Context, address string) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	delete(m.identities, address)
	return nil
}

// IsTrustedIdentity is the only method here that checks the context, so that
// tests can make the encryption fail from inside the per-device loop the same
// way a real (database-backed) store would.
func (m *memoryIdentityStore) IsTrustedIdentity(ctx context.Context, address string, key [32]byte) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	m.lock.Lock()
	defer m.lock.Unlock()
	existing, ok := m.identities[address]
	return !ok || existing == key, nil
}

type emptyLIDStore struct{}

func (emptyLIDStore) PutManyLIDMappings(context.Context, []store.LIDMapping) error { return nil }
func (emptyLIDStore) PutLIDMapping(context.Context, types.JID, types.JID) error    { return nil }

func (emptyLIDStore) GetPNForLID(context.Context, types.JID) (types.JID, error) {
	return types.EmptyJID, nil
}

func (emptyLIDStore) GetLIDForPN(context.Context, types.JID) (types.JID, error) {
	return types.EmptyJID, nil
}

func (emptyLIDStore) GetManyLIDsForPNs(context.Context, []types.JID) (map[types.JID]types.JID, error) {
	return nil, nil
}

func (emptyLIDStore) GetManyPNsForLIDs(context.Context, []types.JID) (map[types.JID]types.JID, error) {
	return nil, nil
}

func newTestClient(tb testing.TB, concurrency int) *Client {
	tb.Helper()
	ownJID := types.JID{User: "9999999999", Device: 0, Server: types.DefaultUserServer}
	device := &store.Device{
		Log:            waLog.Noop,
		IdentityKey:    keys.NewKeyPair(),
		RegistrationID: 1234,
		ID:             &ownJID,
		Identities:     &memoryIdentityStore{identities: make(map[string][32]byte)},
		Sessions:       &memorySessionStore{sessions: make(map[string][]byte)},
		LIDs:           emptyLIDStore{},
	}
	device.SignedPreKey = device.IdentityKey.CreateSignedPreKey(1)
	cli := NewClient(device, waLog.Noop)
	cli.EncryptionConcurrency = concurrency
	return cli
}

// establishSession creates an outgoing signal session with the given address by
// processing a prekey bundle from a freshly generated remote identity.
func establishSession(tb testing.TB, cli *Client, jid types.JID) {
	tb.Helper()
	remoteIdentity := keys.NewKeyPair()
	preKey := keys.NewPreKey(1)
	signedPreKey := remoteIdentity.CreateSignedPreKey(2)
	bundle := prekey.NewBundle(
		4321, uint32(jid.Device),
		optional.NewOptionalUint32(preKey.KeyID), signedPreKey.KeyID,
		ecc.NewDjbECPublicKey(*preKey.Pub), ecc.NewDjbECPublicKey(*signedPreKey.Pub),
		*signedPreKey.Signature,
		identity.NewKey(ecc.NewDjbECPublicKey(*remoteIdentity.Pub)),
	)
	builder := session.NewBuilderFromSignal(cli.Store, jid.SignalAddress(), pbSerializer)
	if err := builder.ProcessBundle(context.Background(), bundle); err != nil {
		tb.Fatalf("failed to establish session with %s: %v", jid, err)
	}
}

func testDeviceJIDs(count int) []types.JID {
	devices := make([]types.JID, count)
	for i := range devices {
		devices[i] = types.JID{
			User:   fmt.Sprintf("100000000%02d", i),
			Device: uint16(i % 4),
			Server: types.DefaultUserServer,
		}
	}
	return devices
}

// testPlaintext returns a plaintext with spare capacity, like the slices
// proto.Marshal returns, so that padMessage writing into the shared backing
// array would show up as a data race.
func testPlaintext() []byte {
	return append(make([]byte, 0, 256), "hello world"...)
}

func nodeJIDs(t *testing.T, nodes []waBinary.Node) []types.JID {
	t.Helper()
	jids := make([]types.JID, len(nodes))
	for i, node := range nodes {
		if node.Tag != "to" {
			t.Fatalf("participant node %d has tag %q, expected \"to\"", i, node.Tag)
		}
		jid, ok := node.Attrs["jid"].(types.JID)
		if !ok {
			t.Fatalf("participant node %d has no JID attribute", i)
		}
		jids[i] = jid
	}
	return jids
}

func TestEncryptMessageForDevicesPreservesOrder(t *testing.T) {
	for _, concurrency := range []int{1, 4, 16} {
		t.Run(fmt.Sprintf("concurrency=%d", concurrency), func(t *testing.T) {
			cli := newTestClient(t, concurrency)
			devices := testDeviceJIDs(50)
			for _, jid := range devices {
				establishSession(t, cli, jid)
			}
			nodes, includeIdentity, err := cli.encryptMessageForDevices(
				t.Context(), devices, "msgid", testPlaintext(), nil, waBinary.Attrs{},
			)
			if err != nil {
				t.Fatalf("encryptMessageForDevices returned an error: %v", err)
			}
			if !includeIdentity {
				t.Error("expected includeIdentity to be true for freshly created sessions")
			}
			if got := nodeJIDs(t, nodes); !slices.Equal(got, devices) {
				t.Errorf("participant nodes are out of order:\n got %v\nwant %v", got, devices)
			}
		})
	}
}

func TestEncryptMessageForDevicesSkipsDevicesWithoutSession(t *testing.T) {
	cli := newTestClient(t, 8)
	devices := testDeviceJIDs(20)
	var expected []types.JID
	for i, jid := range devices {
		// Every third device has no session and no way to fetch a prekey bundle
		// (the client isn't connected), so it must be skipped with a warning.
		if i%3 == 0 {
			continue
		}
		establishSession(t, cli, jid)
		expected = append(expected, jid)
	}
	nodes, _, err := cli.encryptMessageForDevices(
		t.Context(), devices, "msgid", testPlaintext(), nil, waBinary.Attrs{},
	)
	if err != nil {
		t.Fatalf("encryptMessageForDevices returned an error: %v", err)
	}
	if got := nodeJIDs(t, nodes); !slices.Equal(got, expected) {
		t.Errorf("unexpected participant nodes:\n got %v\nwant %v", got, expected)
	}
}

func TestEncryptMessageForDevicesAbortsOnCanceledContext(t *testing.T) {
	cli := newTestClient(t, 8)
	devices := testDeviceJIDs(20)
	for _, jid := range devices {
		establishSession(t, cli, jid)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, _, err := cli.encryptMessageForDevices(ctx, devices, "msgid", testPlaintext(), nil, waBinary.Attrs{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected a context.Canceled error, got %v", err)
	}
}

func TestForEachDeviceGroupRunsEveryIndexInOrder(t *testing.T) {
	cli := newTestClient(t, 8)
	groups := [][]int{{0, 3, 7}, {1}, {2, 5}, {4}, {6}}
	var lock sync.Mutex
	visitedAt := make(map[int]int, 8)
	err := cli.forEachDeviceGroup(t.Context(), groups, func(_ context.Context, i int) error {
		lock.Lock()
		defer lock.Unlock()
		visitedAt[i] = len(visitedAt)
		return nil
	})
	if err != nil {
		t.Fatalf("forEachDeviceGroup returned an error: %v", err)
	}
	visited := slices.Sorted(maps.Keys(visitedAt))
	if want := []int{0, 1, 2, 3, 4, 5, 6, 7}; !slices.Equal(visited, want) {
		t.Errorf("forEachDeviceGroup visited %v, want %v", visited, want)
	}
	for _, group := range groups {
		for i := 1; i < len(group); i++ {
			if visitedAt[group[i-1]] > visitedAt[group[i]] {
				t.Errorf("index %d was visited before %d despite sharing a session", group[i], group[i-1])
			}
		}
	}
}

func TestForEachDeviceGroupPropagatesErrors(t *testing.T) {
	errBoom := errors.New("boom")
	for _, concurrency := range []int{1, 8} {
		t.Run(fmt.Sprintf("concurrency=%d", concurrency), func(t *testing.T) {
			cli := newTestClient(t, concurrency)
			groups := [][]int{{0}, {1}, {2}, {3}}
			err := cli.forEachDeviceGroup(t.Context(), groups, func(_ context.Context, i int) error {
				if i == 2 {
					return errBoom
				}
				return nil
			})
			if !errors.Is(err, errBoom) {
				t.Errorf("expected the callback error to be propagated, got %v", err)
			}
		})
	}
}

func BenchmarkEncryptMessageForDevices(b *testing.B) {
	for _, concurrency := range []int{1, 4, 16} {
		b.Run(fmt.Sprintf("concurrency=%d", concurrency), func(b *testing.B) {
			cli := newTestClient(b, concurrency)
			devices := testDeviceJIDs(500)
			for _, jid := range devices {
				establishSession(b, cli, jid)
			}
			ctx := context.Background()
			plaintext := testPlaintext()
			b.ResetTimer()
			for b.Loop() {
				_, _, err := cli.encryptMessageForDevices(ctx, devices, "msgid", plaintext, nil, waBinary.Attrs{})
				if err != nil {
					b.Fatalf("encryptMessageForDevices returned an error: %v", err)
				}
			}
		})
	}
}
