// Copyright (c) 2026 Tulir Asokan
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package sqlstore

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"io"
	"strings"
	"sync"
	"testing"

	"go.mau.fi/whatsmeow/types"
)

type migrationTestDB struct {
	lock     sync.Mutex
	sessions map[string][]byte
}

type migrationTestConnector struct {
	db *migrationTestDB
}

func (c *migrationTestConnector) Connect(context.Context) (driver.Conn, error) {
	return &migrationTestConn{db: c.db}, nil
}

func (c *migrationTestConnector) Driver() driver.Driver {
	return c
}

func (c *migrationTestConnector) Open(string) (driver.Conn, error) {
	return &migrationTestConn{db: c.db}, nil
}

type migrationTestConn struct {
	db *migrationTestDB
}

func (c *migrationTestConn) Prepare(string) (driver.Stmt, error) {
	return nil, driver.ErrSkip
}

func (c *migrationTestConn) Close() error {
	return nil
}

func (c *migrationTestConn) Begin() (driver.Tx, error) {
	return migrationTestTx{}, nil
}

func (c *migrationTestConn) BeginTx(context.Context, driver.TxOptions) (driver.Tx, error) {
	return migrationTestTx{}, nil
}

func (c *migrationTestConn) QueryContext(_ context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	c.db.lock.Lock()
	defer c.db.lock.Unlock()
	if strings.Contains(query, "SELECT EXISTS") {
		prefix := args[1].Value.(string) + ":"
		hasRows := false
		for address := range c.db.sessions {
			if strings.HasPrefix(address, prefix) {
				hasRows = true
				break
			}
		}
		return &migrationTestRows{columns: []string{"exists"}, values: [][]driver.Value{{hasRows}}}, nil
	}
	if strings.Contains(query, "SELECT session FROM whatsmeow_sessions") {
		session, ok := c.db.sessions[args[1].Value.(string)]
		if !ok {
			return &migrationTestRows{columns: []string{"session"}}, nil
		}
		return &migrationTestRows{columns: []string{"session"}, values: [][]driver.Value{{session}}}, nil
	}
	return &migrationTestRows{columns: []string{"unused"}}, nil
}

func (c *migrationTestConn) ExecContext(_ context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	c.db.lock.Lock()
	defer c.db.lock.Unlock()
	switch {
	case strings.Contains(query, "INSERT INTO whatsmeow_sessions") && strings.Contains(query, "SELECT"):
		pnPrefix := args[1].Value.(string) + ":"
		lidPrefix := args[2].Value.(string) + ":"
		var migrated int64
		for address, session := range c.db.sessions {
			if strings.HasPrefix(address, pnPrefix) {
				c.db.sessions[lidPrefix+strings.TrimPrefix(address, pnPrefix)] = session
				migrated++
			}
		}
		return driver.RowsAffected(migrated), nil
	case strings.Contains(query, "INSERT INTO whatsmeow_sessions"):
		address := args[1].Value.(string)
		c.db.sessions[address] = append([]byte(nil), args[2].Value.([]byte)...)
		return driver.RowsAffected(1), nil
	case strings.Contains(query, "DELETE FROM whatsmeow_sessions"):
		prefix := strings.TrimSuffix(args[1].Value.(string), "%")
		var deleted int64
		for address := range c.db.sessions {
			if strings.HasPrefix(address, prefix) {
				delete(c.db.sessions, address)
				deleted++
			}
		}
		return driver.RowsAffected(deleted), nil
	default:
		return driver.RowsAffected(0), nil
	}
}

type migrationTestTx struct{}

func (migrationTestTx) Commit() error   { return nil }
func (migrationTestTx) Rollback() error { return nil }

type migrationTestRows struct {
	columns []string
	values  [][]driver.Value
	index   int
}

func (r *migrationTestRows) Columns() []string { return r.columns }
func (r *migrationTestRows) Close() error      { return nil }

func (r *migrationTestRows) Next(dest []driver.Value) error {
	if r.index >= len(r.values) {
		return io.EOF
	}
	copy(dest, r.values[r.index])
	r.index++
	return nil
}

func TestMigratePNToLIDRetriesAfterNoRows(t *testing.T) {
	ctx := context.Background()
	testDB := &migrationTestDB{sessions: make(map[string][]byte)}
	db := sql.OpenDB(&migrationTestConnector{db: testDB})
	t.Cleanup(func() { _ = db.Close() })
	container := NewWithDB(db, "postgres", nil)
	store := NewSQLStore(container, types.NewJID("11111", types.DefaultUserServer))
	pn := types.NewJID("22222", types.DefaultUserServer)
	lid := types.NewJID("33333", types.HiddenUserServer)

	const attempts = 8
	start := make(chan struct{})
	errs := make(chan error, attempts)
	var wg sync.WaitGroup
	for range attempts {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			errs <- store.MigratePNToLID(ctx, pn, lid)
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("initial no-op migration failed: %v", err)
		}
	}
	session := []byte("session created after initial migration attempt")
	if err := store.PutSession(ctx, pn.SignalAddress().String(), session); err != nil {
		t.Fatalf("failed to create PN session: %v", err)
	}
	if err := store.MigratePNToLID(ctx, pn, lid); err != nil {
		t.Fatalf("second migration failed: %v", err)
	}
	migrated, err := store.GetSession(ctx, lid.SignalAddress().String())
	if err != nil {
		t.Fatalf("failed to read migrated LID session: %v", err)
	}
	if string(migrated) != string(session) {
		t.Fatalf("unexpected migrated session %q", migrated)
	}
}
