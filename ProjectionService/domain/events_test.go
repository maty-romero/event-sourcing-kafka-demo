package domain

import "testing"

func event(typ EventType, account string, amount int, revision int64) Event {
	e := Event{
		EventID:   "evt-" + string(typ),
		EventType: typ,
		AccountID: account,
		StreamID:  "account-" + account,
		Revision:  revision,
	}
	e.Data.Amount = amount
	return e
}

func TestApplyDepositYWithdraw(t *testing.T) {
	created := event(AccountCreated, "901", 0, 0)
	deposited := event(MoneyDeposited, "901", 100, 1)
	withdrawn := event(MoneyWithdrawn, "901", 40, 2)

	if b, err := Apply(500, created); err != nil || b != 0 {
		t.Fatalf("AccountCreated debe resetear a 0, got %d err %v", b, err)
	}
	if b, err := Apply(0, deposited); err != nil || b != 100 {
		t.Fatalf("deposito 100 sobre 0 debe dar 100, got %d err %v", b, err)
	}
	if b, err := Apply(100, withdrawn); err != nil || b != 60 {
		t.Fatalf("retiro 40 sobre 100 debe dar 60, got %d err %v", b, err)
	}
}

func TestApplyWithdrawInsuficienteEsError(t *testing.T) {
	over := event(MoneyWithdrawn, "901", 1000, 3)
	if _, err := Apply(100, over); err == nil {
		t.Fatal("retiro con saldo insuficiente debe devolver error (-> DLQ)")
	}
}

func TestApplyEventoDesconocidoEsError(t *testing.T) {
	unknown := event(EventType("SomethingElse"), "901", 1, 4)
	if _, err := Apply(0, unknown); err == nil {
		t.Fatal("evento desconocido debe devolver error (-> DLQ)")
	}
}
