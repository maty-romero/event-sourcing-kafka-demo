package domain

import "fmt"

type EventType string

const (
	AccountCreated   EventType = "AccountCreated"
	MoneyDeposited   EventType = "MoneyDeposited"
	MoneyWithdrawn   EventType = "MoneyWithdrawn"
	CurrentSchemaVer           = 1
)

// Event es el envelope V2 que publica el puente ESDB->Kafka. Trae identidad
// (eventId), stream de origen y revision dentro del stream: con eso la
// proyeccion puede deduplicar redeliveries (limitacion 2b de la V1).
type Event struct {
	EventID       string    `json:"eventId"`
	EventType     EventType `json:"eventType"`
	AccountID     string    `json:"accountId"`
	StreamID      string    `json:"streamId"`
	Revision      int64     `json:"revision"`
	SchemaVersion int       `json:"schemaVersion"`
	Timestamp     string    `json:"timestamp"`
	Data          struct {
		Amount int `json:"amount"`
	} `json:"data"`
}

func (e Event) Amount() int { return e.Data.Amount }

// Apply es el fold puro del read model de balances. En V2 es defensivo:
// la invariante de descubierto ya la valida el agregado aguas arriba,
// asi que un error aca significa evento invalido -> DLQ, nunca descarte.
func Apply(currentBalance int, event Event) (int, error) {
	switch event.EventType {
	case AccountCreated:
		return 0, nil
	case MoneyDeposited:
		return currentBalance + event.Amount(), nil
	case MoneyWithdrawn:
		if currentBalance-event.Amount() < 0 {
			return 0, fmt.Errorf("balance insuficiente para cuenta %s", event.AccountID)
		}
		return currentBalance - event.Amount(), nil

	default:
		return currentBalance, fmt.Errorf("evento desconocido: %s", event.EventType)
	}
}
