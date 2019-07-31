# sessionup-pgstore

[![Build status](https://travis-ci.org/swithek/sessionup-pgstore.svg?branch=master)](https://travis-ci.org/swithek/sessionup-pgstore)
[![Go Report Card](https://goreportcard.com/badge/github.com/swithek/sessionup-pgstore)](https://goreportcard.com/report/github.com/swithek/sessionup-pgstore)
[![GoDoc](https://godoc.org/github.com/swithek/sessionup-pgstore?status.png)](https://godoc.org/github.com/swithek/sessionup-pgstore)

PostgreSQL session store implementation for [sessionup](https://github.com/swithek/sessionup)

## Usage
Create and activate a new PgStore:
```go
db, err := sql.Open("postgres", "...")
if err != nil {
      // handle error
}

store, err := pgstore.New(db, "sessions", time.Minute * 5)
if err != nil {
      // handle error
}

manager := sessionup.NewManager(store)
```
