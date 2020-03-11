package pgtestdb

import (
	"database/sql"
	"io"
	"net/url"
	"strings"
	"sync"
)

// Manager .
type Manager struct {
	adminURL *url.URL
	db       *sql.DB
	created  struct {
		mu   sync.Mutex
		data map[string]struct{}
	}
}

var _ io.Closer = (*Manager)(nil)

// New return new Manager
func New(adminConn string) (*Manager, error) {
	var err error

	m := &Manager{}

	m.adminURL, err = url.Parse(adminConn)
	if err != nil {
		return nil, err
	}

	m.db, err = sql.Open("postgres", adminConn)
	if err != nil {
		return nil, err
	}

	m.created.data = make(map[string]struct{})

	return m, nil
}

// Close the manager
func (m *Manager) Close() error {
	var list []string

	func() {
		m.created.mu.Lock()
		defer m.created.mu.Unlock()

		for conn := range m.created.data {
			list = append(list, conn)
		}
	}()

	for _, conn := range list {
		m.Destroy(conn)
	}

	return m.db.Close()
}

// Create will create new database, and return the url to connect to that database
func (m *Manager) Create() (string, error) {
	m.created.mu.Lock()
	defer m.created.mu.Unlock()

	user := "u" + randomHex()
	pass := "p" + randomHex()
	dbname := "d" + randomHex()

	if err := m.createUser(user, pass); err != nil {
		return "", err
	}

	if err := m.createDB(user, dbname); err != nil {
		m.dropUser(user)
		return "", err
	}

	connURL := &url.URL{}
	*connURL = *m.adminURL
	connURL.User = url.UserPassword(user, pass)
	connURL.Path = "/" + dbname
	connURL.RawPath = ""

	conn := connURL.String()

	m.created.data[conn] = struct{}{}

	return conn, nil
}

// Destroy the database that pointed by the url
func (m *Manager) Destroy(conn string) {
	m.created.mu.Lock()
	defer m.created.mu.Unlock()

	uri, err := url.Parse(conn)
	if err != nil {
		return
	}

	user := uri.User.Username()
	dbname := uri.EscapedPath()
	dbname = strings.TrimPrefix(dbname, "/")

	m.dropDB(dbname)
	m.dropUser(user)

	delete(m.created.data, conn)
}
