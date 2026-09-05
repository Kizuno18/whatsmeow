// Copyright (c) 2025 Tulir Asokan
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package store

import (
	"context"
	"fmt"
	"sync"

	"github.com/rs/zerolog"
	"go.mau.fi/libsignal/state/record"
)

type contextKey int

const (
	contextKeySessionCache contextKey = iota
)

type sessionCacheEntry struct {
	Dirty  bool
	Found  bool
	Record *record.Session
}

// sessionCache holds the signal sessions of one send for the duration of that
// send. Only the map is locked, not the entries: a given address is only ever
// touched by one goroutine at a time, because the send holds that address's
// session lock and never encrypts for the same address twice in parallel.
// Locking the whole cache on every session write instead would serialize the
// encryption of large groups.
type sessionCache struct {
	lock    sync.RWMutex
	entries map[string]*sessionCacheEntry
}

func (sc *sessionCache) get(address string) *sessionCacheEntry {
	sc.lock.RLock()
	defer sc.lock.RUnlock()
	return sc.entries[address]
}

func (sc *sessionCache) put(address string, sess *record.Session) {
	if entry := sc.get(address); entry != nil {
		entry.Record = sess
		entry.Found = true
		entry.Dirty = true
		return
	}
	sc.lock.Lock()
	defer sc.lock.Unlock()
	if entry, ok := sc.entries[address]; ok {
		entry.Record = sess
		entry.Found = true
		entry.Dirty = true
		return
	}
	sc.entries[address] = &sessionCacheEntry{Record: sess, Found: true, Dirty: true}
}

func getSessionCache(ctx context.Context) *sessionCache {
	if ctx == nil {
		return nil
	}
	val := ctx.Value(contextKeySessionCache)
	if val == nil {
		return nil
	}
	if cache, ok := val.(*sessionCache); ok {
		return cache
	}
	return nil
}

func getCachedSession(ctx context.Context, addr string) *record.Session {
	cache := getSessionCache(ctx)
	if cache == nil {
		return nil
	}
	entry := cache.get(addr)
	if entry == nil {
		return nil
	}
	return entry.Record
}

func putCachedSession(ctx context.Context, addr string, record *record.Session) bool {
	cache := getSessionCache(ctx)
	if cache == nil {
		return false
	}
	cache.put(addr, record)
	return true
}

func (device *Device) WithCachedSessions(ctx context.Context, addresses []string) (map[string]bool, context.Context, error) {
	if len(addresses) == 0 {
		return nil, ctx, nil
	}

	sessions, err := device.Sessions.GetManySessions(ctx, addresses)
	if err != nil {
		return nil, ctx, fmt.Errorf("failed to prefetch sessions: %w", err)
	}
	wrapped := make(map[string]*sessionCacheEntry, len(sessions))
	existingSessions := make(map[string]bool, len(sessions))
	for addr, rawSess := range sessions {
		var sessionRecord *record.Session
		var found bool
		if rawSess == nil {
			sessionRecord = record.NewSession(SignalProtobufSerializer.Session, SignalProtobufSerializer.State)
		} else {
			found = true
			sessionRecord, err = record.NewSessionFromBytes(rawSess, SignalProtobufSerializer.Session, SignalProtobufSerializer.State)
			if err != nil {
				zerolog.Ctx(ctx).Err(err).
					Str("address", addr).
					Msg("Failed to deserialize session")
				continue
			}
		}
		existingSessions[addr] = found
		wrapped[addr] = &sessionCacheEntry{Record: sessionRecord, Found: found}
	}

	ctx = context.WithValue(ctx, contextKeySessionCache, &sessionCache{entries: wrapped})
	return existingSessions, ctx, nil
}

func (device *Device) PutCachedSessions(ctx context.Context) error {
	cache := getSessionCache(ctx)
	if cache == nil {
		return nil
	}
	cache.lock.RLock()
	dirtySessions := make(map[string][]byte)
	for addr, entry := range cache.entries {
		if entry.Dirty {
			dirtySessions[addr] = entry.Record.Serialize()
		}
	}
	cache.lock.RUnlock()
	if len(dirtySessions) > 0 {
		err := device.Sessions.PutManySessions(ctx, dirtySessions)
		if err != nil {
			return fmt.Errorf("failed to store cached sessions: %w", err)
		}
	}
	cache.lock.Lock()
	clear(cache.entries)
	cache.lock.Unlock()
	return nil
}
