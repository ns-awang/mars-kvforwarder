package config

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	mlog "github.com/netSkope/mars-lib/src/log"
)

const (
	defaultCredsPath = "/vault/secrets/aurora.conf"
	MySQLPort        = 3306
	DefaultDatabase  = "mars"
)

var slog = mlog.GetLogger()

type vaultMySQLCredentials struct {
	Host     string `json:"writer_host"`
	UserName string `json:"user_name"`
	Password string `json:"password"`
}

// MySQLConfig holds the runtime configuration required to connect to MySQL.
type MySQLConfig struct {
	Host     string
	User     string
	Password string
	Database string
	Port     int
}

// LoadMySQLConfig reads credentials from the Vault-managed JSON file.
func LoadMySQLConfig() (MySQLConfig, error) {
	path := strings.TrimSpace(os.Getenv("KVF_MYSQL_CREDENTIALS_PATH"))
	if path == "" {
		path = defaultCredsPath
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		return MySQLConfig{}, fmt.Errorf("read mysql credentials %q: %w", path, err)
	}

	var creds vaultMySQLCredentials
	if err := json.Unmarshal(raw, &creds); err != nil {
		return MySQLConfig{}, fmt.Errorf("parse mysql credentials %q: %w", path, err)
	}

	if err := validateVaultCreds(creds); err != nil {
		return MySQLConfig{}, fmt.Errorf("invalid mysql credentials %q: %w", path, err)
	}

	cfg := MySQLConfig{
		Host:     creds.Host,
		User:     creds.UserName,
		Password: creds.Password,
		Database: DefaultDatabase,
		Port:     MySQLPort,
	}

	slog.Infof("mysql credentials loaded: database=%s", cfg.Database)

	return cfg, nil
}

func validateVaultCreds(c vaultMySQLCredentials) error {
	switch {
	case strings.TrimSpace(c.Host) == "":
		return fmt.Errorf("missing writer_host")
	case strings.TrimSpace(c.UserName) == "":
		return fmt.Errorf("missing user_name")
	case strings.TrimSpace(c.Password) == "":
		return fmt.Errorf("missing password")
	default:
		return nil
	}
}
