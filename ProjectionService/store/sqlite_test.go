package store

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"

	"event_projection/domain"
)

func openTestDB(t *testing.T, kind string) *sql.DB {
	t.Helper()
	dsn := filepath.Join(t.TempDir(), "test.db") + "?_pragma=busy_timeout(5000)"
	db, err := Connect(dsn, kind)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func event(typ domain.EventType, account string, amount int, revision int64) domain.Event {
	e := domain.Event{
		EventID:   fmt.Sprintf("evt-%s-%d", account, revision),
		EventType: typ,
		AccountID: account,
		StreamID:  "account-" + account,
		Revision:  revision,
	}
	e.Data.Amount = amount
	return e
}

func TestApplyEventEsIdempotentePorCheckpoint(t *testing.T) {
	db := openTestDB(t, KindBalances)
	ctx := context.Background()

	created := event(domain.AccountCreated, "901", 0, 0)
	deposited := event(domain.MoneyDeposited, "901", 500, 1)

	if applied, err := ApplyEvent(ctx, db, KindBalances, created); err != nil || !applied {
		t.Fatalf("AccountCreated: applied=%v err=%v", applied, err)
	}
	if applied, err := ApplyEvent(ctx, db, KindBalances, deposited); err != nil || !applied {
		t.Fatalf("MoneyDeposited: applied=%v err=%v", applied, err)
	}

	// Redelivery exacto del deposito: debe ignorarse (limitacion 2b de la V1).
	if applied, err := ApplyEvent(ctx, db, KindBalances, deposited); err != nil || applied {
		t.Fatalf("redelivery no debe aplicar (applied=%v) ni fallar (err=%v)", applied, err)
	}

	if balance, found, err := LookupBalance(ctx, db, "901"); err != nil || !found || balance != 500 {
		t.Fatalf("balance tras reduplicar debe seguir en 500, got %d found=%v err=%v", balance, found, err)
	}
}

func TestEventoInvalidoNoTocaReadModelNiCheckpoint(t *testing.T) {
	db := openTestDB(t, KindBalances)
	ctx := context.Background()

	if _, err := ApplyEvent(ctx, db, KindBalances, event(domain.AccountCreated, "901", 0, 0)); err != nil {
		t.Fatal(err)
	}
	if _, err := ApplyEvent(ctx, db, KindBalances, event(domain.MoneyDeposited, "901", 100, 1)); err != nil {
		t.Fatal(err)
	}

	// Retiro sin saldo: el agregado deberia haberlo bloqueado aguas arriba;
	// si llega, es error -> el consumer lo manda al DLQ y nada se escribe.
	over := event(domain.MoneyWithdrawn, "901", 1000, 2)
	if _, err := ApplyEvent(ctx, db, KindBalances, over); err == nil {
		t.Fatal("retiro invalido debe devolver error")
	}

	if balance, _, err := LookupBalance(ctx, db, "901"); err != nil || balance != 100 {
		t.Fatalf("balance no debe cambiar tras evento invalido, got %d err=%v", balance, err)
	}

	// El checkpoint quedo en 1: el evento 2 puede reintentarse despues.
	ok := event(domain.MoneyWithdrawn, "901", 50, 2)
	if applied, err := ApplyEvent(ctx, db, KindBalances, ok); err != nil || !applied {
		t.Fatalf("evento valido en la misma revision debe aplicar tras el fallo, applied=%v err=%v", applied, err)
	}
	if balance, _, err := LookupBalance(ctx, db, "901"); err != nil || balance != 50 {
		t.Fatalf("balance esperado 50, got %d err=%v", balance, err)
	}
}

func TestStatementProyectaMovimientosEnOrden(t *testing.T) {
	db := openTestDB(t, KindStatement)
	ctx := context.Background()

	if _, err := ApplyEvent(ctx, db, KindStatement, event(domain.AccountCreated, "901", 0, 0)); err != nil {
		t.Fatal(err)
	}
	if _, err := ApplyEvent(ctx, db, KindStatement, event(domain.MoneyDeposited, "901", 500, 1)); err != nil {
		t.Fatal(err)
	}
	// Redelivery: no duplica.
	if applied, err := ApplyEvent(ctx, db, KindStatement, event(domain.MoneyDeposited, "901", 500, 1)); err != nil || applied {
		t.Fatalf("redelivery en statement no debe aplicar, applied=%v err=%v", applied, err)
	}

	movements, err := LookupMovements(ctx, db, "901")
	if err != nil {
		t.Fatal(err)
	}
	if len(movements) != 2 {
		t.Fatalf("esperaba 2 movimientos (sin duplicar), got %d", len(movements))
	}
	if movements[0].Revision != 0 || movements[1].Revision != 1 {
		t.Fatalf("movimientos fuera de orden: %+v", movements)
	}
}
