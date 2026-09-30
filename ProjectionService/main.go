package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"event_projection/api"
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

	consumerDone := make(chan error, 1)
	go func() {
		consumerDone <- consumer.Run(ctx, reader, handle)
	}()

	httpDone := make(chan error, 1)
	go func() {
		httpDone <- api.Run(ctx, getenv("HTTP_ADDR", ":8090"), db)
	}()

	fatal := func(err error, msg string) {
		if err != nil {
			stop()
			log.Fatalf("%s: %v", msg, err)
		}
	}

	// La caida de cualquiera de los dos procesos tira al otro.
	select {
	case err := <-consumerDone:
		fatal(err, "consumer terminó con error")
		<-httpDone
	case err := <-httpDone:
		fatal(err, "HTTP de lecturas terminó con error")
		<-consumerDone
	}
	stop()
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
