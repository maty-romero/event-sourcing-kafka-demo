package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"event_projection/consumer"
	"event_projection/domain"
	"event_projection/store"

	_ "modernc.org/sqlite"

	"github.com/segmentio/kafka-go"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	db, err := store.Connect(getenv("DB_PATH", "data.db"))
	if err != nil {
		log.Fatalf("conectar sqlite: %v", err)
	}
	defer db.Close()

	brokers := strings.Split(getenv("KAFKA_BROKERS", "localhost:9092"), ",")
	topic := getenv("KAFKA_TOPIC", "account-events")
	groupID := getenv("KAFKA_GROUP", "balance-projection-group")

	startOffset := kafka.LastOffset
	if strings.EqualFold(getenv("KAFKA_START_OFFSET", "last"), "first") {
		startOffset = kafka.FirstOffset
	}

	reader := consumer.NewReader(brokers, topic, groupID, startOffset)
	defer reader.Close()

	handle := func(event domain.Event) error {
		current, err := store.GetBalance(ctx, db, event.AccountID)
		if err != nil {
			return err
		}
		newBalance, err := domain.Apply(current, event)
		if err != nil {
			return err
		}
		return store.SaveBalance(ctx, db, event.AccountID, newBalance)
	}

	if err := consumer.Run(ctx, reader, handle); err != nil {
		log.Fatalf("consumer terminó con error: %v", err)
	}
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
