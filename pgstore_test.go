package pgstore

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"net"
	"reflect"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/lib/pq"
	"github.com/swithek/sessionup"
)

var terr = errors.New("error")

func TestType(t *testing.T) {
	defer func() {
		if err := recover(); err != nil {
			t.Fatalf("want nil, got %v", err)
		}
	}()
	var _ sessionup.Store = &PgStore{}
}

func TestNew(t *testing.T) {
	type check func(*testing.T, *PgStore, error)

	checks := func(cc ...check) []check { return cc }

	hasErr := func(exp error) check {
		return func(t *testing.T, _ *PgStore, err error) {
			if exp != err {
				t.Errorf("want %v, got %v", exp, err)
			}
		}
	}

	hasStore := func(tN string, db, errChan, stopChan bool) check {
		return func(t *testing.T, pg *PgStore, _ error) {
			if tN == "" && !db && !errChan && !stopChan {
				return
			}

			if pg == nil {
				t.Fatal("want non-nil, got nil")
			}

			if db && pg.db == nil {
				t.Error("want non-nil, got nil")
			}

			if pg.tName != tN {
				t.Errorf("want %q, got %q", tN, pg.tName)
			}

			if errChan && pg.errChan == nil {
				t.Error("want non-nil, got nil")
			}

			if stopChan && pg.stopChan != nil {
				t.Error("want nil, got non-nil")
			}

			pg.StopCleanup()
		}
	}

	db, mock := mockDB(t)
	defer db.Close()
	tName := "sessions"
	q := fmt.Sprintf(table, tName)

	cc := map[string]struct {
		Expect   func()
		Duration time.Duration
		Checks   []check
	}{
		"Error returned during table creation": {
			Expect: func() {
				mock.ExpectExec(q).WillReturnError(terr)
			},
			Duration: time.Hour,
			Checks: checks(
				hasErr(terr),
				hasStore("", false, false, false),
			),
		},
		"Successful init without cleanup": {
			Expect: func() {
				mock.ExpectExec(q).WillReturnResult(sqlmock.NewResult(0, 0))
			},
			Checks: checks(
				hasErr(nil),
				hasStore(tName, true, true, false),
			),
		},
		"Successful init with cleanup": {
			Expect: func() {
				mock.ExpectExec(q).WillReturnResult(sqlmock.NewResult(0, 0))
			},
			Duration: time.Hour,
			Checks: checks(
				hasErr(nil),
				hasStore(tName, true, true, true),
			),
		},
	}

	for cn, c := range cc {
		t.Run(cn, func(t *testing.T) {
			c.Expect()
			pg, err := New(db, tName, c.Duration)
			for _, ch := range c.Checks {
				ch(t, pg, err)
			}

			if err = mock.ExpectationsWereMet(); err != nil {
				t.Errorf("want nil, got %v", err)
			}
		})
	}
}

func TestCreate(t *testing.T) {
	db, mock := mockDB(t)
	defer db.Close()
	tName := "sessions"
	pg := PgStore{db: db, tName: tName}
	q := fmt.Sprintf("INSERT INTO %s VALUES ($1, $2, $3, $4, $5, $6, $7);", tName)

	s := sessionup.Session{
		CreatedAt: time.Now(),
		ExpiresAt: time.Now(),
		ID:        "id",
		UserKey:   "key",
		IP:        net.ParseIP("127.0.0.1"),
	}
	s.Agent.OS = "GNU/Linux"
	s.Agent.Browser = "Firefox"

	cc := map[string]struct {
		Expect func()
		Err    error
	}{
		"Duplicate ID": {
			Expect: func() {
				mock.ExpectExec(q).
					WithArgs(s.CreatedAt, s.ExpiresAt, s.ID, s.UserKey,
						s.IP.String(), s.Agent.OS, s.Agent.Browser).
					WillReturnError(&pq.Error{Constraint: fmt.Sprintf("%s_pkey", tName)})
			},
			Err: sessionup.ErrDuplicateID,
		},
		"Error returned during create": {
			Expect: func() {
				mock.ExpectExec(q).
					WithArgs(s.CreatedAt, s.ExpiresAt, s.ID, s.UserKey,
						s.IP.String(), s.Agent.OS, s.Agent.Browser).
					WillReturnError(terr)
			},
			Err: terr,
		},
		"Successful create": {
			Expect: func() {
				mock.ExpectExec(q).
					WithArgs(s.CreatedAt, s.ExpiresAt, s.ID, s.UserKey,
						s.IP.String(), s.Agent.OS, s.Agent.Browser).
					WillReturnResult(sqlmock.NewResult(0, 1))
			},
		},
	}

	for cn, c := range cc {
		t.Run(cn, func(t *testing.T) {
			c.Expect()
			err := pg.Create(context.Background(), s)
			if err != c.Err {
				t.Errorf("want %v, got %v", c.Err, err)
			}

			if err = mock.ExpectationsWereMet(); err != nil {
				t.Errorf("want nil, got %v", err)
			}
		})
	}

}

func TestFetchByID(t *testing.T) {
	type check func(*testing.T, sessionup.Session, bool, error)

	checks := func(cc ...check) []check { return cc }

	hasErr := func(exp error) check {
		return func(t *testing.T, _ sessionup.Session, _ bool, err error) {
			if exp != err {
				t.Errorf("want %v, got %v", exp, err)
			}
		}
	}

	hasSession := func(exp sessionup.Session, found bool) check {
		return func(t *testing.T, s sessionup.Session, ok bool, err error) {
			if found != ok {
				t.Errorf("want %t, got %t", found, ok)
			}

			if !reflect.DeepEqual(exp, s) {
				t.Errorf("want %v, got %v", exp, s)
			}
		}
	}

	db, mock := mockDB(t)
	defer db.Close()
	tName := "sessions"
	pg := PgStore{db: db, tName: tName}
	q := fmt.Sprintf("SELECT * FROM %s WHERE id = $1 AND expires_at > CURRENT_TIMESTAMP;", tName)

	s := sessionup.Session{
		CreatedAt: time.Now(),
		ExpiresAt: time.Now(),
		ID:        "id",
		UserKey:   "key",
		IP:        net.ParseIP("127.0.0.1"),
	}
	s.Agent.OS = "GNU/Linux"
	s.Agent.Browser = "Firefox"

	cc := map[string]struct {
		Expect func()
		Checks []check
	}{
		"sql.ErrNoRows returned during select": {
			Expect: func() {
				mock.ExpectQuery(q).WithArgs(s.ID).WillReturnError(sql.ErrNoRows)
			},
			Checks: checks(
				hasErr(nil),
				hasSession(sessionup.Session{}, false),
			),
		},
		"Error returned during select": {
			Expect: func() {
				mock.ExpectQuery(q).WithArgs(s.ID).WillReturnError(terr)
			},
			Checks: checks(
				hasErr(terr),
				hasSession(sessionup.Session{}, false),
			),
		},
		"Successful select": {
			Expect: func() {
				rows := sqlmock.NewRows([]string{"created_at", "expires_at", "id",
					"user_key", "ip", "agent_os", "agent_browser"}).
					AddRow(s.CreatedAt, s.ExpiresAt, s.ID, s.UserKey,
						s.IP.String(), s.Agent.OS, s.Agent.Browser)
				mock.ExpectQuery(q).WithArgs(s.ID).WillReturnRows(rows)
			},
			Checks: checks(
				hasErr(nil),
				hasSession(s, true),
			),
		},
	}

	for cn, c := range cc {
		t.Run(cn, func(t *testing.T) {
			c.Expect()
			s1, ok, err := pg.FetchByID(context.Background(), s.ID)
			for _, ch := range c.Checks {
				ch(t, s1, ok, err)
			}

			if err = mock.ExpectationsWereMet(); err != nil {
				t.Errorf("want nil, got %v", err)
			}
		})
	}
}

func TestFetchByUserKey(t *testing.T) {
	type check func(*testing.T, []sessionup.Session, error)

	checks := func(cc ...check) []check { return cc }

	hasErr := func(exp error) check {
		return func(t *testing.T, _ []sessionup.Session, err error) {
			if exp != err {
				t.Errorf("want %v, got %v", exp, err)
			}
		}
	}

	hasSessions := func(exp []sessionup.Session) check {
		return func(t *testing.T, ss []sessionup.Session, err error) {
			if !reflect.DeepEqual(exp, ss) {
				t.Errorf("want %v, got %v", exp, ss)
			}
		}
	}

	db, mock := mockDB(t)
	defer db.Close()
	key := "key"
	tName := "sessions"
	pg := PgStore{db: db, tName: tName}
	q := fmt.Sprintf("SELECT * FROM %s WHERE user_key = $1;", tName)

	gen := func() []sessionup.Session {
		var res []sessionup.Session
		for i := 0; i < 3; i++ {
			res = append(res, sessionup.Session{ID: fmt.Sprintf("id%d", i)})
		}
		return res
	}

	cc := map[string]struct {
		Expect func()
		Checks []check
	}{
		"sql.ErrNoRows returned during select": {
			Expect: func() {
				mock.ExpectQuery(q).WithArgs(key).WillReturnError(sql.ErrNoRows)
			},
			Checks: checks(
				hasErr(nil),
				hasSessions(nil),
			),
		},
		"Error returned during select": {
			Expect: func() {
				mock.ExpectQuery(q).WithArgs(key).WillReturnError(terr)
			},
			Checks: checks(
				hasErr(terr),
				hasSessions(nil),
			),
		},
		"Successful select": {
			Expect: func() {
				rows := sqlmock.NewRows([]string{"created_at", "expires_at", "id",
					"user_key", "ip", "agent_os", "agent_browser"})
				for _, s := range gen() {
					rows.AddRow(s.CreatedAt, s.ExpiresAt, s.ID, s.UserKey, s.IP,
						s.Agent.OS, s.Agent.Browser)
				}
				mock.ExpectQuery(q).WithArgs(key).WillReturnRows(rows)
			},
			Checks: checks(
				hasErr(nil),
				hasSessions(gen()),
			),
		},
	}

	for cn, c := range cc {
		t.Run(cn, func(t *testing.T) {
			c.Expect()
			ss, err := pg.FetchByUserKey(context.Background(), key)
			for _, ch := range c.Checks {
				ch(t, ss, err)
			}

			if err = mock.ExpectationsWereMet(); err != nil {
				t.Errorf("want nil, got %v", err)
			}
		})
	}
}

func TestDeleteByID(t *testing.T) {
	db, mock := mockDB(t)
	defer db.Close()
	id := "id"
	tName := "sessions"
	pg := PgStore{db: db, tName: tName}
	q := fmt.Sprintf("DELETE FROM %s WHERE id = $1;", tName)

	// 1
	mock.ExpectExec(q).WithArgs(id).WillReturnError(terr)
	err := pg.DeleteByID(context.Background(), id)
	if err != terr {
		t.Errorf("want %v, got %v", terr, err)
	}

	if err = mock.ExpectationsWereMet(); err != nil {
		t.Errorf("want nil, got %v", err)
	}

	// 2
	mock.ExpectExec(q).WithArgs(id).WillReturnResult(sqlmock.NewResult(0, 1))
	err = pg.DeleteByID(context.Background(), id)
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}

	if err = mock.ExpectationsWereMet(); err != nil {
		t.Errorf("want nil, got %v", err)
	}
}

func TestDeleteByUserKey(t *testing.T) {
	db, mock := mockDB(t)
	defer db.Close()
	tName := "sessions"
	key := "key"
	ids := []string{"id1", "id2", "id3"}
	pg := PgStore{db: db, tName: tName}

	cc := map[string]struct {
		Expect func()
		ExpIDs []string
		Err    error
	}{
		"Error returned during delete": {
			Expect: func() {
				q := fmt.Sprintf("DELETE FROM %s WHERE user_key = $1;", tName)
				mock.ExpectExec(q).
					WithArgs(key).WillReturnError(terr)
			},
			Err: terr,
		},
		"Error returned during delete with exceptions": {
			Expect: func() {
				q := fmt.Sprintf("DELETE FROM %s WHERE user_key = $1 AND id != ALL ($2);", tName)
				mock.ExpectExec(q).
					WithArgs(append([]driver.Value{key}, pq.Array(ids))...).
					WillReturnError(terr)
			},
			ExpIDs: ids,
			Err:    terr,
		},
		"Successful delete": {
			Expect: func() {
				q := fmt.Sprintf("DELETE FROM %s WHERE user_key = $1;", tName)
				mock.ExpectExec(q).
					WithArgs(key).
					WillReturnResult(sqlmock.NewResult(0, 1))
			},
		},
		"Successful delete with exceptions": {
			Expect: func() {
				q := fmt.Sprintf("DELETE FROM %s WHERE user_key = $1 AND id != ALL ($2);", tName)
				mock.ExpectExec(q).
					WithArgs(append([]driver.Value{key}, pq.Array(ids))...).
					WillReturnResult(sqlmock.NewResult(0, 1))
			},
			ExpIDs: ids,
		},
	}

	for cn, c := range cc {
		t.Run(cn, func(t *testing.T) {
			c.Expect()
			err := pg.DeleteByUserKey(context.Background(), key, c.ExpIDs...)
			if err != c.Err {
				t.Errorf("want %v, got %v", c.Err, err)
			}

			if err = mock.ExpectationsWereMet(); err != nil {
				t.Errorf("want nil, got %v", err)
			}
		})
	}
}

func TestDeleteExpired(t *testing.T) {
	db, mock := mockDB(t)
	defer db.Close()
	tName := "sessions"
	pg := PgStore{db: db, tName: tName}
	q := fmt.Sprintf("DELETE FROM %s WHERE expires_at < CURRENT_TIMESTAMP;", tName)

	// 1
	mock.ExpectExec(q).WillReturnError(terr)
	err := pg.deleteExpired()
	if err != terr {
		t.Errorf("want %v, got %v", terr, err)
	}

	if err = mock.ExpectationsWereMet(); err != nil {
		t.Errorf("want nil, got %v", err)
	}

	// 2
	mock.ExpectExec(q).WillReturnResult(sqlmock.NewResult(0, 1))
	err = pg.deleteExpired()
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}

	if err = mock.ExpectationsWereMet(); err != nil {
		t.Errorf("want nil, got %v", err)
	}
}

func TestSetNullString(t *testing.T) {
	s := setNullString("")
	if s.Valid {
		t.Errorf("want %t, got %t", false, s.Valid)
	}

	if s.String != "" {
		t.Errorf("want %q, got %q", "", s.String)
	}

	s = setNullString("test")
	if !s.Valid {
		t.Errorf("want %t, got %t", true, s.Valid)
	}

	if s.String == "" {
		t.Errorf("want %q, got %q", "test", s.String)
	}
}

func mockDB(t *testing.T) (*sql.DB, sqlmock.Sqlmock) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatalf("want nil, got %v", err)
	}
	return db, mock
}
