package store

import (
	"context"
	"database/sql"
	"fmt"

	"event_projection/domain"

	_ "modernc.org/sqlite"
)

const (
	KindBalances  = "balances"
	KindStatement = "statement"
)

// Connect abre el SQLite de la proyeccion y crea sus tablas: el read model
// del kind correspondiente y la tabla de checkpoints. La proyeccion es la
// unica duena de su base (una DB por proyeccion).
func Connect(dsn, kind string) (*sql.DB, error) {
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("abrir sqlite: %w", err)
	}

	stmts := []string{
		`CREATE TABLE IF NOT EXISTS checkpoints (
			stream_id     TEXT PRIMARY KEY,
			last_revision INTEGER NOT NULL
		)`,
	}

	switch kind {
	case KindBalances:
		stmts = append(stmts, `
		CREATE TABLE IF NOT EXISTS account_balances (
			account_id TEXT PRIMARY KEY,
			balance    INTEGER NOT NULL
		)`)
	case KindStatement:
		stmts = append(stmts, `
		CREATE TABLE IF NOT EXISTS movements (
			event_id   TEXT PRIMARY KEY,
			account_id TEXT NOT NULL,
			revision   INTEGER NOT NULL,
			event_type TEXT NOT NULL,
			amount     INTEGER NOT NULL,
			timestamp  TEXT NOT NULL
		)`)
	default:
		return nil, fmt.Errorf("kind de proyeccion desconocido: %q", kind)
	}

	for _, s := range stmts {
		if _, err := db.ExecContext(context.Background(), s); err != nil {
			return nil, fmt.Errorf("crear esquema de %s: %w", kind, err)
		}
	}

	return db, nil
}

// ApplyEvent aplica un evento al read model junto con su checkpoint, en la
// MISMA transaccion. Devuelve applied=false si el evento ya fue procesado
// (revision <= checkpoint del stream): redelivery no altera el estado.
// Un error deja la transaccion intacta: read model y checkpoint nunca se
// desincronizan.
func ApplyEvent(ctx context.Context, db *sql.DB, kind string, event domain.Event) (bool, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	last, err := lastRevision(ctx, tx, event.StreamID)
	if err != nil {
		return false, err
	}
	if event.Revision <= last {
		return false, nil
	}

	switch kind {
	case KindBalances:
		if err := applyBalance(ctx, tx, event); err != nil {
			return false, err
		}
	case KindStatement:
		if err := applyMovement(ctx, tx, event); err != nil {
			return false, err
		}
	default:
		return false, fmt.Errorf("kind de proyeccion desconocido: %q", kind)
	}

	if _, err := tx.ExecContext(ctx, `
		INSERT INTO checkpoints (stream_id, last_revision)
		VALUES (?, ?)
		ON CONFLICT(stream_id) DO UPDATE SET last_revision = excluded.last_revision
	`, event.StreamID, event.Revision); err != nil {
		return false, fmt.Errorf("guardar checkpoint: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("commit: %w", err)
	}
	return true, nil
}

func lastRevision(ctx context.Context, tx *sql.Tx, streamID string) (int64, error) {
	var last int64
	err := tx.QueryRowContext(ctx,
		`SELECT last_revision FROM checkpoints WHERE stream_id = ?`, streamID,
	).Scan(&last)

	if err == sql.ErrNoRows {
		return -1, nil
	}
	if err != nil {
		return 0, fmt.Errorf("leer checkpoint de %s: %w", streamID, err)
	}
	return last, nil
}

func applyBalance(ctx context.Context, tx *sql.Tx, event domain.Event) error {
	var current int
	err := tx.QueryRowContext(ctx,
		`SELECT balance FROM account_balances WHERE account_id = ?`, event.AccountID,
	).Scan(&current)
	if err != nil && err != sql.ErrNoRows {
		return fmt.Errorf("leer balance de %s: %w", event.AccountID, err)
	}

	newBalance, err := domain.Apply(current, event)
	if err != nil {
		return err
	}

	_, err = tx.ExecContext(ctx, `
		INSERT INTO account_balances (account_id, balance)
		VALUES (?, ?)
		ON CONFLICT(account_id) DO UPDATE SET balance = excluded.balance
	`, event.AccountID, newBalance)
	if err != nil {
		return fmt.Errorf("guardar balance de %s: %w", event.AccountID, err)
	}
	return nil
}

func applyMovement(ctx context.Context, tx *sql.Tx, event domain.Event) error {
	_, err := tx.ExecContext(ctx, `
		INSERT INTO movements (event_id, account_id, revision, event_type, amount, timestamp)
		VALUES (?, ?, ?, ?, ?, ?)
	`, event.EventID, event.AccountID, event.Revision, string(event.EventType),
		event.Amount(), event.Timestamp)
	if err != nil {
		return fmt.Errorf("registrar movimiento de %s: %w", event.AccountID, err)
	}
	return nil
}

// LookupBalance distingue "no existe" de "existe con balance 0".
func LookupBalance(ctx context.Context, db *sql.DB, accountID string) (int, bool, error) {
	var balance int
	err := db.QueryRowContext(ctx,
		`SELECT balance FROM account_balances WHERE account_id = ?`, accountID,
	).Scan(&balance)

	switch {
	case err == sql.ErrNoRows:
		return 0, false, nil
	case err != nil:
		return 0, false, err
	default:
		return balance, true, nil
	}
}

type Movement struct {
	EventID   string `json:"eventId"`
	Revision  int64  `json:"revision"`
	EventType string `json:"eventType"`
	Amount    int    `json:"amount"`
	Timestamp string `json:"timestamp"`
}

func LookupMovements(ctx context.Context, db *sql.DB, accountID string) ([]Movement, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT event_id, revision, event_type, amount, timestamp
		FROM movements WHERE account_id = ?
		ORDER BY revision ASC
	`, accountID)
	if err != nil {
		return nil, fmt.Errorf("leer movimientos de %s: %w", accountID, err)
	}
	defer rows.Close()

	movements := []Movement{}
	for rows.Next() {
		var m Movement
		if err := rows.Scan(&m.EventID, &m.Revision, &m.EventType, &m.Amount, &m.Timestamp); err != nil {
			return nil, err
		}
		movements = append(movements, m)
	}
	return movements, rows.Err()
}
