package store

import (
	"context"
	"database/sql"
	"fmt"
)

func Connect(dsn string) (*sql.DB, error) {
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("abrir sqlite: %w", err)
	}

	if _, err := db.ExecContext(context.Background(), `
		CREATE TABLE IF NOT EXISTS account_balances (
			account_id TEXT PRIMARY KEY,
			balance    INTEGER NOT NULL
		)
	`); err != nil {
		return nil, fmt.Errorf("crear tabla account_balances: %w", err)
	}

	return db, nil
}

func GetBalance(ctx context.Context, db *sql.DB, accountID string) (int, error) {
	var balance int
	err := db.QueryRowContext(ctx,
		`SELECT balance FROM account_balances WHERE account_id = ?`, accountID,
	).Scan(&balance)

	if err == sql.ErrNoRows {
		return 0, nil
	}
	return balance, err
}

func SaveBalance(ctx context.Context, db *sql.DB, accountID string, balance int) error {
	_, err := db.ExecContext(ctx, `
		INSERT INTO account_balances (account_id, balance)
		VALUES (?, ?)
		ON CONFLICT(account_id) DO UPDATE SET balance = excluded.balance
	`, accountID, balance)
	return err
}
