package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"sessionup"
	"strings"
	"time"
)

type postgres struct {
	db      *sql.DB
	tName   string
	stop    chan struct{}
	errChan chan error
}

func New(db *sql.DB, tName string, d time.Duration) (*postgres, error) {
	p := &postgres{db: db, tName: tName, errChan: make(chan error)}
	if err := p.createTable(); err != nil {
		return nil, err
	}

	if d > 0 {
		go p.cleanUp(d)
	}
	return p, nil
}

func (p *postgres) createTable() error {
	q := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		expires TIMESTAMPTZ NOT NULL,
		id TEXT PRIMARY KEY,
		user_key TEXT NOT NULL,
		ip TEXT,
		agent_os TEXT,
		agent_browser TEXT
	);
	CREATE INDEX expiration ON %s (expires);`, p.tName, p.tName)
	_, err := p.db.Exec(q)
	return err
}

func (p *postgres) Create(ctx context.Context, s sessionup.Session) error {
	q := fmt.Sprintf("INSERT INTO %s VALUES ($1, $2, $3, $4, $5, $6);", p.tName)
	_, err := p.db.ExecContext(ctx, q, s.Expires, s.ID, s.UserKey, setNullString(s.IP.String()),
		setNullString(s.Agent.OS), setNullString(s.Agent.Browser))
	return err
}

func (p *postgres) FetchByID(ctx context.Context, id string) (sessionup.Session, bool, error) {
	q := fmt.Sprintf("SELECT * FROM %s WHERE id = $1 AND expires > CURRENT_TIMESTAMP;", p.tName)
	r := p.db.QueryRowContext(ctx, q, id)

	var s sessionup.Session
	var ip, os, browser sql.NullString

	err := r.Scan(&s.Expires, &s.ID, &s.UserKey, &ip, &os, &browser)
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

func (p *postgres) FetchByUserKey(ctx context.Context, key string) ([]sessionup.Session, error) {
	q := fmt.Sprintf("SELECT * FROM %s WHERE user_key = $s;", p.tName)
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

		err = rr.Scan(&s.Expires, &s.ID, &s.UserKey, &ip, &os, &browser)
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

func (p *postgres) DeleteByID(ctx context.Context, id string) error {
	q := fmt.Sprintf("DELETE FROM %s WHERE id = $1;", p.tName)
	_, err := p.db.ExecContext(ctx, q, id)
	return err
}

func (p *postgres) DeleteByUserKey(ctx context.Context, key string, expID ...string) error {
	var b strings.Builder
	b.WriteString(fmt.Sprintf("DELETE FROM %s WHERE user_key = $1", p.tName))
	if len(expID) > 0 {
		b.WriteString(fmt.Sprintf(" AND NOT IN (%s)", strings.Join(expID, ", ")))
	}
	b.WriteByte(';')
	_, err := p.db.ExecContext(ctx, b.String(), append([]string{key}, expID...))
	return err
}

func (p *postgres) deleteExpired() error {
	q := fmt.Sprintf("DELETE FROM %s WHERE expires < CURRENT_TIMESTAMP;", p.tName)
	_, err := p.db.Exec(q)
	return err
}

func (p *postgres) cleanUp(d time.Duration) {
	p.stop = make(chan struct{})
	t := time.NewTicker(d)
	for {
		select {
		case <-t.C:
			if err := p.deleteExpired(); err != nil {
				p.errChan <- err
			}
		case <-p.stop:
			t.Stop()
			return
		}
	}
}

func (p *postgres) CleanUpErr() <-chan error {
	return p.errChan
}

func setNullString(s string) sql.NullString {
	var ns sql.NullString
	if s != "" {
		ns.String = s
		ns.Valid = true
	}
	return ns
}
