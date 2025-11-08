package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLoadMySQLConfigSuccess(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "creds.json")
	err := os.WriteFile(path, []byte(`{
		"writer_host": "mysql-primary.internal",
		"user_name": "kvforwarder",
		"password": "s3cr3t"
	}`), 0o600)
	require.NoError(t, err)

	t.Setenv("KVF_MYSQL_CREDENTIALS_PATH", path)

	cfg, err := LoadMySQLConfig()
	require.NoError(t, err)
	require.Equal(t, "mysql-primary.internal", cfg.Host)
	require.Equal(t, "kvforwarder", cfg.User)
	require.Equal(t, "s3cr3t", cfg.Password)
	require.Equal(t, DefaultDatabase, cfg.Database)
	require.Equal(t, MySQLPort, cfg.Port)
}

func TestLoadMySQLConfigMissingField(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "creds.json")
	err := os.WriteFile(path, []byte(`{
		"writer_host": "mysql-primary.internal",
		"user_name": "kvforwarder"
	}`), 0o600)
	require.NoError(t, err)

	t.Setenv("KVF_MYSQL_CREDENTIALS_PATH", path)

	_, err = LoadMySQLConfig()
	require.Error(t, err)
	require.Contains(t, err.Error(), "missing password")
}

func TestLoadMySQLConfigMissingFile(t *testing.T) {
	t.Setenv("KVF_MYSQL_CREDENTIALS_PATH", "/nonexistent/path/mysql.json")
	_, err := LoadMySQLConfig()
	require.Error(t, err)
	require.Contains(t, err.Error(), "read mysql credentials")
}
