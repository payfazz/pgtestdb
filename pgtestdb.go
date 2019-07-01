package pgtestdb

import (
	"database/sql"
	"fmt"
	"io"
	"net/url"
)

// Manager .
type Manager struct {
	adminURI *url.URL
	db       *sql.DB
}

var _ io.Closer = (*Manager)(nil)

// New .
func New(adminURI string) (*Manager, error) {
	var err error

	ret := &Manager{}

	ret.adminURI, err = url.Parse(adminURI)
	if err != nil {
		return nil, err
	}
	if ret.adminURI.Scheme != "postgres" {
		return nil, fmt.Errorf("uri scheme must postgres")
	}

	db, err := sql.Open("postgres", adminURI)
	if err != nil {
		return nil, err
	}
	ret.db = db

	return ret, nil
}

// Close .
func (m *Manager) Close() error {
	return m.db.Close()
}

// Create .
func (m *Manager) Create() (*url.URL, error) {
	user := "u" + randomHex()
	pass := "p" + randomHex()
	dbname := "d" + randomHex()
	if err := m.createUser(user, pass); err != nil {
		return nil, err
	}
	if err := m.createDB(user, dbname); err != nil {
		m.dropUser(user)
		return nil, err
	}

	retURL := new(url.URL)
	*retURL = *m.adminURI
	retURL.User = url.UserPassword(user, pass)
	retURL.Path = "/" + dbname
	retURL.RawPath = ""
	return retURL, nil
}

// Destroy .
func (m *Manager) Destroy(uri *url.URL) error {
	user := uri.User.Username()
	dbname := uri.Path
	if len(dbname) > 0 {
		dbname = dbname[1:]
	}
	m.dropDB(dbname)
	m.dropUser(user)
	return nil
}
