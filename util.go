package pgtestdb

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
)

func randomHex() string {
	data := [8]byte{}
	for {
		_, err := rand.Read(data[:])
		if err == nil {
			return hex.EncodeToString(data[:])
		}
	}
}

func (m *Manager) createUser(user, pass string) error {
	if _, err := m.db.Exec(
		fmt.Sprintf("create user %s with encrypted password '%s';",
			user, pass,
		),
	); err != nil {
		return err
	}
	return nil
}

func (m *Manager) dropUser(user string) error {
	if _, err := m.db.Exec(
		fmt.Sprintf("drop user %s;", user),
	); err != nil {
		return err
	}
	return nil
}

func (m *Manager) createDB(user, dbname string) error {
	if _, err := m.db.Exec(
		fmt.Sprintf("create database %s owner %s;",
			dbname, user,
		),
	); err != nil {
		return err
	}
	return nil
}

func (m *Manager) dropDB(dbname string) error {
	if _, err := m.db.Exec(
		fmt.Sprintf("drop database %s;", dbname),
	); err != nil {
		return err
	}
	return nil
}
