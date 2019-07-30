package pgstore

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"time"

	"github.com/lib/pq"
	"github.com/swithek/sessionup"
)

const table = `CREATE TABLE IF NOT EXISTS %s (
	created_at TIMESTAMPTZ NOT NULL,
	expires_at TIMESTAMPTZ NOT NULL,
	id TEXT PRIMARY KEY,
	user_key TEXT NOT NULL,
	ip TEXT,
	agent_os TEXT,
	agent_browser TEXT
);`

// PgStore is a PostgreSQL implementation of sessionup.Store.
type PgStore struct {
	db       *sql.DB
	tName    string
	stopChan chan struct{}
	errChan  chan error
}

// New returns a fresh instance of PgStore.
// tName parameter determines the name of the table that
// will be used for sessions. If it does not exist, it will
// be created.
// Duration parameter determines how often the cleanup
// function wil be called to remove the expired sessions.
// Setting it to 0 will prevent cleanup from being activated.
func New(db *sql.DB, tName string, d time.Duration) (*PgStore, error) {
	p := &PgStore{db: db, tName: tName, errChan: make(chan error)}
	_, err := p.db.Exec(fmt.Sprintf(table, p.tName))
	if err != nil {
		return nil, err
	}

	if d > 0 {
		go p.startCleanup(d)
	}
	return p, nil
}

// Create implements sessionup.Store interface's Create method.
func (p *PgStore) Create(ctx context.Context, s sessionup.Session) error {
	q := fmt.Sprintf("INSERT INTO %s VALUES ($1, $2, $3, $4, $5, $6, $7);", p.tName)
	_, err := p.db.ExecContext(ctx, q, s.CreatedAt, s.ExpiresAt, s.ID, s.UserKey,
		setNullString(s.IP.String()), setNullString(s.Agent.OS), setNullString(s.Agent.Browser))
	if perr, ok := err.(*pq.Error); ok && perr.Constraint == fmt.Sprintf("%s_pkey", p.tName) {
		return sessionup.ErrDuplicateID
	}
	return err
}

// FetchByID implements sessionup.Store interface's FetchByID method.
func (p *PgStore) FetchByID(ctx context.Context, id string) (sessionup.Session, bool, error) {
	q := fmt.Sprintf("SELECT * FROM %s WHERE id = $1 AND expires_at > CURRENT_TIMESTAMP;", p.tName)
	r := p.db.QueryRowContext(ctx, q, id)

	var s sessionup.Session
	var ip, os, browser sql.NullString

	err := r.Scan(&s.CreatedAt, &s.ExpiresAt, &s.ID, &s.UserKey, &ip, &os, &browser)
	if err == sql.ErrNoRows {
		return sessionup.Session{}, false, nil
	} else if err != nil {
		return sessionup.Session{}, false, err
	}

	if ip.Valid {
		s.IP = net.ParseIP(ip.String)
	}

	s.Agent.OS = os.String
	s.Agent.Browser = browser.String
	return s, true, nil
}

// FetchByUserKey implements sessionup.Store interface's FetchByUserKey method.
func (p *PgStore) FetchByUserKey(ctx context.Context, key string) ([]sessionup.Session, error) {
	q := fmt.Sprintf("SELECT * FROM %s WHERE user_key = $1;", p.tName)
	rr, err := p.db.QueryContext(ctx, q, key)
	if err == sql.ErrNoRows {
		return nil, nil
	} else if err != nil {
		return nil, err
	}

	var ss []sessionup.Session
	for rr.Next() {
		var s sessionup.Session
		var ip, os, browser sql.NullString

		err = rr.Scan(&s.CreatedAt, &s.ExpiresAt, &s.ID, &s.UserKey, &ip, &os, &browser)
		if err != nil {
			rr.Close()
			return nil, err
		}

		if ip.Valid {
			s.IP = net.ParseIP(ip.String)
		}

		s.Agent.OS = os.String
		s.Agent.Browser = browser.String

		ss = append(ss, s)
	}

	if err = rr.Err(); err != nil {
		return nil, err
	}

	return ss, nil
}

// DeleteByID implements sessionup.Store interface's DeleteByID method.
func (p *PgStore) DeleteByID(ctx context.Context, id string) error {
	q := fmt.Sprintf("DELETE FROM %s WHERE id = $1;", p.tName)
	_, err := p.db.ExecContext(ctx, q, id)
	return err
}

// DeleteByUserKey implements sessionup.Store interface's DeleteByUserKey method.
func (p *PgStore) DeleteByUserKey(ctx context.Context, key string, expID ...string) error {
	if len(expID) > 0 {
		q := fmt.Sprintf("DELETE FROM %s WHERE user_key = $1 AND id != ALL ($2);", p.tName)
		_, err := p.db.ExecContext(ctx, q, append([]interface{}{key}, pq.Array(expID))...)
		return err
	}

	q := fmt.Sprintf("DELETE FROM %s WHERE user_key = $1;", p.tName)
	_, err := p.db.ExecContext(ctx, q, key)
	return err
}

// deleteExpired deletes all expired sessions.
func (p *PgStore) deleteExpired() error {
	q := fmt.Sprintf("DELETE FROM %s WHERE expires_at < CURRENT_TIMESTAMP;", p.tName)
	_, err := p.db.Exec(q)
	return err
}

// startCleanup activates repeated sessions' checking and
// deletion process.
func (p *PgStore) startCleanup(d time.Duration) {
	p.stopChan = make(chan struct{})
	t := time.NewTicker(d)
	for {
		select {
		case <-t.C:
			if err := p.deleteExpired(); err != nil {
				p.errChan <- err
			}
		case <-p.stopChan:
			t.Stop()
			return
		}
	}
}

// StopCleanup terminates the automatic cleanup process.
// Useful for testing and cases when store is used only temporary.
// In order to restart the cleanup, new store must be created.
func (p *PgStore) StopCleanup() {
	if p.stopChan != nil {
		p.stopChan <- struct{}{}
	}
}

// CleanupErr returns a receive-only channel to get errors
// produced during the automatic cleanup.
// NOTE: channel must be drained in order for the cleanup
// process to be able to continue.
func (p *PgStore) CleanupErr() <-chan error {
	return p.errChan
}

// setNullString creates sql.NullString from the input string.
func setNullString(s string) sql.NullString {
	var ns sql.NullString
	if s != "" && s != "<nil>" {
		ns.String = s
		ns.Valid = true
	}
	return ns
}
