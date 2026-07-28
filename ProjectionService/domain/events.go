package domain

import "fmt"

type EventType string

const (
	AccountCreated EventType = "AccountCreated"
	MoneyDeposited EventType = "MoneyDeposited"
	MoneyWithdrawn EventType = "MoneyWithdrawn"
)

type Event struct {
	EventType EventType `json:"eventType"`
	AccountID string    `json:"accountId"`
	Amount    int       `json:"amount,omitempty"`
}

func Apply(currentBalance int, event Event) (int, error) {
	switch event.EventType {
	case AccountCreated:
		return 0, nil
	case MoneyDeposited:
		return currentBalance + event.Amount, nil
	case MoneyWithdrawn:
		if currentBalance-event.Amount < 0 {
			return 0, fmt.Errorf("balance insuficiente para cuenta")
		}
		return currentBalance - event.Amount, nil

	default:
		return currentBalance, fmt.Errorf("evento desconocido: %s", event.EventType)
	}
}
